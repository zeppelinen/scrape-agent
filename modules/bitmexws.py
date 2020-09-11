import json
import threading
import sys
import os
import datetime
import traceback
import uuid
from cerberus import Validator
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from qagentws import *
from syshelper import *
from qredis import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_trades = os.environ['TOPIC_TRADES_PREFIX'] + '-' + os.environ['ENV']#'trades-qfx-data-dev'

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}
redis = QRedis(redis_config)

class BitmexWS(QAgentWS):
	def __init__(self, args):

		#parent interface init
		try:
			self.config = args['config']

			#init metrics gathering
			self.metrics = QMetrics({'job_key': 'job_trades'})
			self.counter_scraped_granular = self.metrics.create_gauge('trades_scraped_granular', 'Granular trades scraped', ['exchange', 'market'])
			self.counter_scraped_total = self.metrics.create_gauge('trades_scraped_total', 'Amount of books scraped successfully', ['exchange'])

			args['ws_config'] = {}
			args['ws_config']['url'] = self.config['url']

			QAgentWS.__init__(self, args)
		except Exception as ex:
			self.log.write({
				'exchange': self.exchange,
				'message': "An exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
				'error': traceback.format_exc(),
			}, 'ERROR')

	def push_metrics(self):
		self.metrics.gauge_set(self.counter_scraped_total, *[self.exchange], value=self.r.data_accumulate_counter)
		
		job_key = self.metrics.push_metrics()
		print('PUSHED SOME METRICS')
		self.r.append_pg_key(job_key)

		#reset granular gauge
		self.counter_scraped_granular = self.metrics.reinit_gauge(self.counter_scraped_granular, 'trades_scraped_granular', 'Granular trades scraped', ['exchange', 'market'])
		#reset accumulate counter
		self.r.data_scrape_counter_reset()

	def schedule_reconcilication(self):
		now = int(datetime.datetime.utcnow().timestamp()*1000)
		cmd = "python historical/schedule_trades.py {exchange} {module} {market}\
		\"https://www.bitmex.com/api/v1/trade?symbol=[MARKET]&count=500&reverse=false&startTime=[STARTTIME]&endTime=[ENDTIME]&start=[START]\" \
		{starttime} {endtime} {step} {cutoff}".format(
			exchange=self.config['exchange'],
			module="bitmextradesrest",
			market=self.config['market'],
			starttime=(now-(self.reconciliation + 30)*1000),
			endtime=now,
			step=1800000,
			cutoff=360
		)
		self.log.write('[RECONCILIATION]: {cmd}'.format(cmd=cmd), 'DEBUG')
		run_cmd(cmd, shell=True)

	def msg_received(self, msg):
		QAgentWS.msg_received(self, msg)

		try:
			data = json.loads(msg)
		except ValueError:
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON data malformed',
				'error': traceback.format_exc(),
				'value': repr(msg),
			}, 'ERROR')
			return 0

		#skip intro message
		if 'info' in data:
			return 0

		#skip success subscribe message
		if 'subscribe' in data:
			#print('subscribed response: {success} to {topic}'.format(success=data['success'], topic=data['subscribe']))
			return 0

		if 'data' not in data:
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON datatypes missmatch',
				'error': traceback.format_exc(),
				'value': repr(data),
			}, 'ERROR')
			return 0

		v_schema = {
			'timestamp': {'type': 'string'},
			'symbol': {'type': 'string'},
			'size': {'type': 'float'},
			'price': {'type': 'float'},
			'trdMatchID': {'type': 'string'},
		}
		v = Validator()
		v.allow_unknown = True

		for item in data['data']:
			if not v.validate(item, v_schema):
				self.log.write({
					'exchange': self.exchange,
					'message': 'JSON datatypes missmatch',
					'error': repr(v.errors),
				}, 'ERROR')
			else:
				#skip size=0 ticks
				if item['size'] > 0:
					if item['side'] == "Buy":
						quantity = item['size']
					else:
						quantity = -1 * item['size']
					trade = {
						"exchange": self.exchange,
						"market": item['symbol'],
						"agent_id": self.r.agent_key,
						"message_id": uuid.uuid4().hex,
						"external_timestamp": item['timestamp'],
						"external_id": str(item['trdMatchID']),
						"price": item['price'],
						"quantity": quantity,
						"original_response": json.dumps(item)
					}
					self.pubsub.write(trade)
					#tracking data scraped
					self.r.data_scraped(1)
					self.metrics.gauge_inc(self.counter_scraped_granular, *[self.exchange, item['symbol']])

def listener_thread(args):
	job_key = args['job_key']
	module_config = redis.get_agent_config(job_key)

	ws = BitmexWS({
		"config": module_config,
		"container_name": args['container_name'],
		"log": {
			"name": "agent-log",
			"write_severity": "ALL",
		},
		"redis": redis_config,
		"topic_name": pubsub_topic_trades,
		"reconciliation": 300
	})

def push_command_thread(job_key, command):
	#delay writing to Redis by 10 seconds, allow WS to connect
	waiting = 1
	max_waiting = 10
	while waiting <= max_waiting:
		time.sleep(1)
		redis_key = redis.get_dict(job_key)
		if redis_key is not None:
			break
		waiting = waiting + 1

	command = command.encode('utf-8')
	redis.push_msg_pool(job_key, command)

class BitmexWS_start():
	def __init__(self, args):

		job_key = args['job_key']
		module_config = redis.get_agent_config(job_key)

		listener = threading.Thread(target=listener_thread, args=(args,))

		job_key = redis.generate_key(module_config)
		command = '{"op": "subscribe", "args": ["trade"]}'
		push_command = threading.Thread(
			target=push_command_thread, 
			args=(job_key, command)
		)

		listener.start() 
		push_command.start() 

		listener.join() 
		push_command.join()


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Bitmex trade WS agent')
	parser.add_argument('job_key', help='Redis job key, string, required')
	parser.add_argument('container_name', help='Unique name of container, string, required')
	args = parser.parse_args()
	BitmexWS_start({
		"job_key": args.job_key,
		"container_name": args.container_name,
	})