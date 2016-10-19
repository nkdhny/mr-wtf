import luigi
import datetime
from .metrics import TotalHitsTask, UniqueUsersTask
from config import AppConfig
import shutil
import luigi.task

class LocalMetric(luigi.task):

    date=luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))


    def _depends_on(self):
        raise NotImplemented

    def requires(self):
        return self._depends_on()

    def output(self):
        return luigi.LocalTarget(path='{}/{}_{}'.format(AppConfig.metrics_path, type(self).__name__, self.date))

    def run(self):
        with self.input().open('r') as i:
            with self.output().open('w') as o:
                shutil.copyfileobj(i, o)


class LocalTotalHitsMetric(LocalMetric):
    def _depends_on(self):
        return TotalHitsTask(date=self.date)


class LocalTotalUsersMetric(LocalMetric):
    def _depends_on(self):
        UniqueUsersTask(date=self.date)


class AllMetrics(luigi.WrapperTask):
    date=luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        inst = lambda x: x(date=self.date)
        return [inst(x) for x in LocalMetric.__subclasses__()]