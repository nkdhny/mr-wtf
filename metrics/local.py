import luigi
import datetime
from .hadoop import TotalHitsTask, UniqueUsersTask, SessionLengthTask, UsersByCountryMetric, NewUsersMetric, \
    FacebookConversionsRatio, ProfileHits, ProfileUsers, ProfileView, ProfileLikeMetric, LikedProfiles
from config import AppConfig
import shutil
import luigi.task

class LocalMetric(luigi.Task):

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
        return UniqueUsersTask(date=self.date)


class LocalUserSessionLength(LocalMetric):
    def _depends_on(self):
        return SessionLengthTask(date=self.date)


class LocalUsersByCountry(LocalMetric):
    def _depends_on(self):
        return UsersByCountryMetric(date=self.date)


class LocalNewUsers(LocalMetric):
    def _depends_on(self):
        return NewUsersMetric(date=self.date)


class LocalFacebokokConversionRatio(LocalMetric):
    def _depends_on(self):
        return FacebookConversionsRatio(date=self.date)

class LocalLikes(LocalMetric):
    def _depends_on(self):
        return LikedProfiles(date=self.date)


class AllMetrics(luigi.WrapperTask):
    date=luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        inst = lambda x: x(date=self.date)
        hw_1_metrics = [inst(x) for x in LocalMetric.__subclasses__()]
        hw_2_metrics = [ProfileHits(self.date), ProfileUsers(self.date), ProfileView(self.date),
                        ProfileLikeMetric(self.date)]

        return hw_1_metrics + hw_2_metrics