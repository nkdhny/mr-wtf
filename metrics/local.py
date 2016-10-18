import luigi
import datetime
from .metrics import TotalHitsTask
from config import *
import shutil

class LocalTotalHitsMetric(luigi.Task):
	date=luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=1))

	def requres(self):
		return TotalHitsTask(date=self.date)

	def output(self):
		return luigi.LocalTarget(path='{}/total_hits_{}'.format(self.date))

	def run(self):
		with self.input().open() as i:
			with self.output().open('w') as o:
				shutil.copyfileobj(i, o)