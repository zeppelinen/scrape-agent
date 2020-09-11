from qrequest import *
from qredis import *
from gclog import *
from gcpub import *
from threading import Thread
import traceback
import time
import datetime
import sys
import os
from qmetrics import *
from qmysql import *

class QAgentRequest(Qrequest):
	
	num_fetch_threads = 30
	cut_off_window = 60 #secs, time window after which agent will stop retrying failed requests
	started_at = datetime.datetime.utcnow().timestamp()
	
	def __init__(self, args):
		
		if 'threads' in args['config']:
			self.num_fetch_threads = int(args['config']['threads'])
		if 'cut_off_window' in args['config']:
			self.cut_off_window = int(args['config']['cut_off_window'])
		if 'disable_cut_off_report' in args['config']:
			self.disable_cut_off_report = args['disable_cut_off_report']
		else:
			self.disable_cut_off_report = False

		self.exchange = args['config']['exchange']
		self.log = GCLog(args['log'])
		self.r = QRedis(args['redis'])
		self.r.init_key(args['config'])
		self.r.set_agent_container(self.r.agent_key, args['container_name'])

		self.pubsub = GCPub({
			'topic_name': args['topic_name'],
			'simulate': False
		})
		
		#parent http request init
		Qrequest.__init__(self, args)

		self.queue = Queue()

		#prepopulate the queue
		self.add_urls(args['urls_list'])

		monitor = Thread(target=self.monitor_cut_off)
		monitor.setDaemon(True)
		monitor.start()

		if len(args['urls_list']) < self.num_fetch_threads:
			self.num_fetch_threads = len(args['urls_list'])

		for i in range(self.num_fetch_threads):
			worker = Thread(target=self.consume, args=(i,))
			worker.setDaemon(True)
			worker.start()

		self.queue.join()
		self.log.write('wait to finish pubsub queue', 'DEBUG')
		if hasattr(self, 'processed_urls_counter'):
			self.log.write('WE HAVE PARSED {len} URLS'.format(len=self.processed_urls_counter), 'DEBUG')
		self.wait_pub_sub_queue()

	def wait_pub_sub_queue(self):
		while True:
			time.sleep(0.5)
			if self.pubsub.is_queue_empty():
				time.sleep(2)
				self.push_metrics()
				self.log.write('GRACEFULLY EXITING', 'DEBUG')
				return

	def monitor_cut_off(self):
		while True:
			time.sleep(0.5)
			if self.cut_off():
				if self.disable_cut_off_report is True:
					self.cut_off_triggered = True
				self.push_metrics()
				time.sleep(2)
				self.log.write('FORCED STOPPED DUE TO MONITOR TIMEOUT', 'DEBUG')
				os._exit(0)

	def cut_off(self):
		return (datetime.datetime.utcnow().timestamp() - self.started_at > self.cut_off_window)

	def add_urls(self, urls):
		for url in urls:
			self.put_in_queue(url)

	def put_in_queue(self, url):
		self.queue.put(url)

	def task_done(self):
		self.queue.task_done()

	def is_queue_empty(self):
		return self.queue.empty()

	def consume(self, i):
		while True:
			#print('{i}: LOOKING FOR NEXT URL'.format(i=i))
			try:
				url = self.queue.get()
				self.parse_url(url=url, callback=self.task_done)
			except Exception as ex:
				self.log.write({
					'exchange': self.exchange,
					'message': "An exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
					'error': traceback.format_exc(),
				}, 'ERROR')
				os._exit(0)

	def push_metrics(self):
		self.log.write('QUEUED: {queued} PUBLISHED: {published}'.format(queued=self.pubsub.total_queued, published=self.pubsub.total_published), 'DEBUG')
		job_key = self.metrics.push_metrics()
		self.log.write('PUSHED SOME METRICS', 'DEBUG')
		self.r.append_pg_key(job_key)

	def mysql_finish_job(self, container_name):
		if hasattr(self, 'cut_off_triggered'):
			self.log.write('SKIPPING MYSQL UPDATE, JOB WASN\'T FINISHED PROPERLY', 'DEBUG')
		else:
			#write agent status to DB
			db = QMySql()
			db.connect()
			sql_update = "UPDATE `trades_period` SET `status`='FINISHED', `finished_at`=NOW() WHERE `container_name`=%(container_name)s"
			params = {'container_name': container_name}
			db.execute(sql_update, params)
			db.commit()
			db.close()

			self.log.write('UPDATED RECORD IN DB FOR CONTAINER {container_name}'.format(container_name=container_name), 'DEBUG')