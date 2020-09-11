from qrequest import *
from qredis import *
from qmetrics import *
from gclog import *
from gcpub import *
import os
import time

class QAgentMisc(Qrequest):

	metrics_update_threshold = 5
	num_fetch_threads = 30
	started_at = datetime.datetime.utcnow().timestamp()
	parse_frequency = 60
	
	def __init__(self, args):

		if 'threads' in args['config']:
			self.num_fetch_threads = int(args['config']['threads'])
		if 'parse_frequency' in args['config']:
			self.parse_frequency = args['config']['parse_frequency']

		self.exchange = args['config']['exchange']
		self.log = GCLog(args['log'])
		self.r = QRedis(args['redis'])
		#initial insert/update of a key, this Redis obj copy will now work with current key only
		self.r.init_key(args['config'])
		self.r.set_agent_container(self.r.agent_key, args['container_name'])

		self.pubsub = GCPub({'topic_name': args['topic_name']})

		#parent http request init
		Qrequest.__init__(self, args)

		self.queue = Queue()

		#monitor = Thread(target=self.run_forever)
		#monitor.setDaemon(True)
		#monitor.start()

		for i in range(self.num_fetch_threads):
			worker = Thread(target=self.consume, args=(i,))
			worker.setDaemon(True)
			worker.start()

		#self.queue.join()

		self.run_forever()

	def run_forever(self):
		counter = 1
		update_frequency = 0.5
		try:
			while True:
				counter += 1

				if (counter*update_frequency % self.metrics_update_threshold == 0):
					self.push_metrics()

				#pull data will trigger every X seconds AND at first iteration of monitor
				if (counter*update_frequency % self.parse_frequency == 0 or counter == 2):
					self.pull_data()

				#reading messages queue from Redis
				time.sleep(update_frequency)
		except KeyboardInterrupt:
			print('force stopped')

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