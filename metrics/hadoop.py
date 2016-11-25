#!/usr/bin/env python
import happybase
import luigi
import luigi.contrib.hadoop
import re
import datetime
import random
import bisect

_profile_request_re = re.compile(r'/id(\d+)(\?like=1)?$')

def parse_profile(raw_req):
    m = _profile_request_re.match(raw_req['file'])

    if m is None:
        raw_req['profile'] = None
        raw_req['like'] = None

    else:
        gr = m.groups()

        raw_req['profile'] = int(gr[0])
        raw_req['like'] = (gr[1] is not None)

    return raw_req

def parse_line(line):
    pat = '([(\d\.)]+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'

    match = re.match(pat, line)

    if match is None:
        return {
            'code': None,
            'profile': None
        }

    record = match.groups()

    req_time = datetime.datetime.strptime(record[1][:-6], "%d/%b/%Y:%H:%M:%S")

    raw_req = {
        'code': int(record[3]),
        'ip': record[0],
        'referrer': record[5],
        'req': record[2],
        'file': (record[2]).split(' ')[1],
        #-6 due to dropping tz
        'epoch': (req_time - datetime.datetime(year=1970, month=1, day=1)).total_seconds(),
        'date': req_time.date().strftime('%Y-%m-%d'),
        'hour': req_time.hour
    }

    return parse_profile(raw_req)


class LogFile(luigi.ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.date.strftime('/user/sandello/logs/access.log.%Y-%m-%d'))

class DictFile(luigi.ExternalTask):
    hdfs_path = luigi.Parameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.hdfs_path)


class Metric(luigi.contrib.hadoop.JobTask):
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        return LogFile(self.date)


class DerivativeMetric(luigi.contrib.hadoop.JobTask):
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

class ExternalMetric(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        return LogFile(self.date)

class ExternalMetricWithLag(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))
    lag = 1

    def requires(self):
        return [LogFile(self.date - datetime.timedelta(days=x)) for x in range(self.lag)]


class TotalHitsTask(Metric):
    n_reduce_tasks = 1

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/total_hits_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def mapper(self, line):
        if parse_line(line)['code'] == 200:
            yield "total_hits", 1        

    def reducer(self, key, values):
        yield key, sum(values)

    combiner = reducer


class UniqueUsersTask(Metric):
    n_reduce_tasks = 1

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/total_users_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def mapper(self, line):
        record = parse_line(line)
        if record['code'] == 200:
            yield record['ip'], 1

    def init_reducer(self):
        self.total_users = 0

    def reducer(self, key, values):
        self.total_users += 1
        return []

    def final_reducer(self):
        yield "total_users", self.total_users


class MarkUserSessionTask(ExternalMetric):

    n_reduce_tasks = 3

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/user_sessions_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def run(self):
        from .streaming.sessions import run

        run.run_map_reduce(
                self.input().path, self.output().path, self.task_id,
                self.n_reduce_tasks, '/home/agolomedov/hw1/mr-wtf/metrics/streaming/sessions')


class SessionLengthTask(DerivativeMetric):

    n_reduce_tasks = 1

    def requires(self):
        return MarkUserSessionTask(date=self.date)

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
            "/user/agolomedov/user_session_length_{}".format(self.date),
            format=luigi.contrib.hdfs.PlainDir
        )

    def mapper(self, line):
        ip, sessions = line.split()
        sessions = int(sessions)

        yield "sessions", '{};{}'.format(sessions, 1)

    def combiner(self, key, values):
        weight = 0
        values_sum = 0
        for v_w in values:
            v, w = v_w.split(';')
            v = float(v)
            w = float(w)

            weight += w
            values_sum += w * v

        yield key, '{};{}'.format(values_sum / float(weight), weight)

    def reducer(self, key, values):
        weight = 0
        values_sum = 0
        for v_w in values:
            v, w = v_w.split(';')
            v = float(v)
            w = float(w)

            weight += w
            values_sum += w * v

        yield key, values_sum / float(weight)

class UniqueUsersAdresses(Metric):
    n_reduce_tasks = 3

    def mapper(self, line):
        record = parse_line(line)

        if record['code'] == 200:
            yield record['ip'], 'nothing'

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/unique_user_addresses_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def reducer(self, key, values):
        yield key, 'nothing'

    combiner = reducer


class UsersByCountryMetric(DerivativeMetric):

    locations_file='loc-dict.csv'

    n_reduce_tasks = 1

    def extra_files(self):
        return [('/hdfs/user/sandello/dicts/IP2LOCATION-LITE-DB1.CSV', 'loc-dict.csv')]

    def requires(self):
        return UniqueUsersAdresses(date=self.date)

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/users_by_country_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    @staticmethod
    def parse_location_row(location_row):
        record = location_row.split(',')

        return {
            'lo':  int(record[0].replace('"', '')),
            'hi':  int(record[1].replace('"', '')),
            'country': record[3].replace('"', '').strip()
        }

    def init_mapper(self, location_file_ob=None):
        location_file_ob = location_file_ob or file(self.locations_file)
        self.locations = sorted(
                [self.parse_location_row(x) for x in location_file_ob.readlines()], key=lambda r: r['lo'])
        self.locations_begs = [x['lo'] for x in self.locations]

    @staticmethod
    def ip2code(ip):
        bytes = [int(x) for x in ip.split('.')]
        return bytes[0] << 24 | bytes[1] << 16 | bytes[2] << 8 | bytes[3] << 0


    def find_country(self, ip):
        code = UsersByCountryMetric.ip2code(ip)
        code_idx = bisect.bisect(self.locations_begs, code) - 1
        assert code_idx >= 0

        return self.locations[code_idx]['country']

    def mapper(self, line):
        record = line.split('\t')

        country = self.find_country(record[0])
        yield country, 1

    def reducer(self, key, values):
        yield key, sum(values)

    combiner = reducer


class NewUsersMetric(ExternalMetricWithLag):

    n_reduce_tasks = 1
    lag=14

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/new_users_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def run(self):
        from .streaming.new_users import run

        run.run_map_reduce(
                [x.path for x in self.input()], self.output().path, self.date, self.task_id,
                self.n_reduce_tasks, '/home/agolomedov/hw1/mr-wtf/metrics/streaming/new_users')

class FacebookActions(ExternalMetricWithLag):

    lag = 16
    n_reduce_tasks = 15

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/facebook_actions_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def run(self):
        from .streaming.facebook_conversions import run

        run.run_map_reduce(
                [x.path for x in self.input()], self.output().path, self.date - datetime.timedelta(days=2),
                self.date, self.task_id,
                self.n_reduce_tasks, '/home/agolomedov/hw1/mr-wtf/metrics/streaming/facebook_conversions')


class FacebookConversionsRatio(DerivativeMetric):
    n_reduce_tasks = 1

    def requires(self):
        return FacebookActions(date=self.date)

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/facebook_conversions_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def mapper(self, line):
        _, action = line.split()

        yield action, 1

    def combiner(self, key, vals):
        yield key, sum(vals)

    def init_reducer(self):
        self._num_converted = 0
        self._num_transferred = 0

    def reducer(self, key, vals):
        if key == 'converted':
            self._num_converted += sum(vals)
        if key == 'transferred':
            self._num_transferred += sum(vals)

        return []

    def final_reducer(self):
        if (self._num_transferred > 0):
            yield 'facebook_conversion_ratio', self._num_converted / float(self._num_transferred)
        else:
            yield 'facebook_conversion_ratio', 0


class ProfileHits(Metric):

    n_reduce_tasks = 5

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/profile_hits_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def run(self):
        from .streaming.profile_hits import run

        run.run_map_reduce(
                self.input().path, self.output().path, self.task_id,
                self.n_reduce_tasks, '/home/agolomedov/hw1/mr-wtf/metrics/streaming/profile_hits')


class ProfileUsers(Metric):

    n_reduce_tasks = 15

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/profile_users_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def run(self):
        from .streaming.profile_users import run

        run.run_map_reduce(
                self.input().path, self.output().path, self.task_id,
                self.n_reduce_tasks, '/home/agolomedov/hw1/mr-wtf/metrics/streaming/profile_users')


class ProfileView(Metric):

    n_reduce_tasks = 15

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/profile_views_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def run(self):
        from .streaming.profile_views import run

        run.run_map_reduce(
                self.input().path, self.output().path, self.date, self.task_id,
                self.n_reduce_tasks, '/home/agolomedov/hw1/mr-wtf/metrics/streaming/profile_views')

def compose_table_name(metric):
    return "bigdatashad_{}_{}".format("agolomedov", metric)

def choose_hbase_host():
    HOSTS = ["hadoop2-%02d.yandex.ru" % i for i in xrange(11, 14)]
    return random.choice(HOSTS)


class ProfileLikeMetric(Metric):
    n_reduce_tasks = 5

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(
                "/user/agolomedov/facebook_likes_{}".format(self.date),
                format=luigi.contrib.hdfs.PlainDir
        )

    def mapper(self, line):
        rec = parse_line(line)

        if rec['code'] == 200 and rec['profile'] is not None and rec['like'] is True:
            yield rec['profile'], '{}#{}'.format(rec['ip'], rec['epoch'])

    def init_reducer(self):
        self._table = happybase.Connection(choose_hbase_host()).table(compose_table_name("profilelike"))

    def reducer(self, profile, vals):
        for v in vals:
            ip, epoch = v.split('#')

            self._table.put(b"{}#{}+{}".format(profile, self.date, ip), {b't:ts': epoch})

            yield profile, ip, epoch