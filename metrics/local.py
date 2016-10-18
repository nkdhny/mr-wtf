import luigi
import datetime
from .metrics import TotalHitsTask
from config import AppConfig
import shutil

class LocalTotalHitsMetric(luigi.Task):
    date=luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

    def requires(self):
        return TotalHitsTask(date=self.date)

    def output(self):
        return luigi.LocalTarget(path='{}/total_hits_{}'.format(AppConfig.metrics_path, self.date))

    def run(self):
        with self.input().open('r') as i:
            with self.output().open('w') as o:
                shutil.copyfileobj(i, o)
