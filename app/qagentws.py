from qsocket import *
from qredis import *
from qmetrics import *
from gclog import *
from gcpub import *
import os
import time

class QAgentWS(Qsocket):
	
	metrics_update_threshold = 5
	
	def __init__(self, args):
		self.exchange = args['config']['exchange']
		self.log = GCLog(args['log'])
		self.r = QRedis(args['redis'])
		#initial insert/update of a key, this Redis obj copy will now work with current key only
		self.r.init_key(args['config'])
		self.r.set_agent_container(self.r.agent_key, args['container_name'])

		#reconcilication optional procedure
		if 'reconciliation' in args:
			self.reconciliation = args['reconciliation']
		else:
			self.reconciliation = None

		self.pubsub = GCPub({'topic_name': args['topic_name']})

		#parent web socket start init
		Qsocket.__init__(self, args['ws_config'])

		#make it run forever
		self.run_forever()

	def run_forever(self):
		counter = 1
		update_frequency = 0.5
		try:
			while True:
				counter += 1
				#updating metrics
				self.r.recalc_data_metrics()

				if (counter*update_frequency % self.metrics_update_threshold == 0):
					self.push_metrics()

				#reconciliation will trigger every X seconds AND at first iteration of monitor
				if self.reconciliation is not None:
					if (counter*update_frequency % self.reconciliation == 0 or counter == 2):
						self.schedule_reconcilication()

				#reading messages queue from Redis
				time.sleep(update_frequency)
				msg = self.r.get_msg_pool()
				if msg is not None:
					self.msg_send(msg)
				else:
					#print('NAH, NOTHING IN REDIS QUEUE')
					pass
		except KeyboardInterrupt:
			print('force stopped')

	def msg_received(self, arg):
		self.r.upd_msg_uptime()
		#Qsocket.msg_received(self, arg)