import json
import threading
import sys
import os
import datetime
import time
import traceback
import uuid
from cerberus import Validator
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from qagentws import *
from syshelper import *
from qredis import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_trades = os.environ['TOPIC_TRADES_PREFIX'] + '-' + os.environ['ENV']

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}
redis = QRedis(redis_config)

class BitfinexWS(QAgentWS):

	channel_ids = {}
	max_old_delta = 3600 #secs, older messages will be ignored

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
		\"https://api-pub.bitfinex.com/v2/trades/t[MARKET]/hist?limit=5000&sort=1&start=[STARTTIME]&end=[ENDTIME]\" \
		{starttime} {endtime} {step} {cutoff}".format(
			exchange=self.config['exchange'],
			module="bitfinextradesrest",
			market=self.config['market'],
			starttime=(now-(self.reconciliation + 30)*1000),
			endtime=now,
			step=1800000,
			cutoff=360
		)
		self.log.write('[RECONCILIATION]: {cmd}'.format(cmd=cmd), 'DEBUG')
		run_cmd(cmd, shell=True)

	def get_market_by_channel_id(self, channel_id):
		if channel_id not in self.channel_ids:
			self.log.write({
				'exchange': self.exchange,
				'message': 'Unknown channel id: '+channel_id,
				'error': traceback.format_exc(),
			}, 'ERROR')
		else:
			return self.channel_ids[channel_id]

	def process_trade(self, channel_id, item):
		if len(item) != 4:
			self.log.write({
				'exchange': self.exchange,
				'message': 'LIST datatypes missmatch',
				'error': repr(item),
			}, 'ERROR')
		else:
			now = datetime.datetime.utcnow()
			external_timestamp = datetime.datetime.utcfromtimestamp(int(item[1])/1000)

			#skip old trades snapshots
			if ((now-external_timestamp).total_seconds() > self.max_old_delta):
				return

			market = self.get_market_by_channel_id(channel_id)
			external_id = str(item[0])
			external_timestamp = datetime.datetime.utcfromtimestamp(int(item[1])/1000).isoformat()
			price = float(item[3])
			quantity = float(item[2])
			trade = {
				"exchange": self.exchange,
				"market": market,
				"agent_id": self.r.agent_key,
				"message_id": uuid.uuid4().hex,
				"external_timestamp": external_timestamp,
				"external_id": external_id,
				"price": price,
				"quantity": quantity,
				"original_response": json.dumps(item)
			}
			#print(trade)
			self.r.data_scraped(1)
			self.pubsub.write(trade)
			self.metrics.gauge_inc(self.counter_scraped_granular, *[self.exchange, market])


	def msg_received(self, msg):
		QAgentWS.msg_received(self, msg)

		#print('RECEIVING: '+repr(msg))

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

		#processing WS formatted messages
		if type(data) is dict:
			if 'event' in data:
				event = data['event']
				#skip intro message
				if event == 'info':
					print('SKIP INTRO MESSAGE')
					return 0

				if event == 'pong':
					print('PONG MESSAGE RECEIVED')
					return 0

				#map channel id and market
				if event == 'subscribed':
					self.channel_ids[data['chanId']] = data['pair']
					print('CHANNEL ID SET')
					print(self.channel_ids)
					return 0

		if type(data) is list:
			try:
				channel_id = data[0]
				second_argument = data[1]

				#skip heartbeat message
				if second_argument == "hb":
					#print('HEARTBEAT RECEIVED')
					return 0

				#process individual update messages
				if second_argument in ["tu"]:
					self.process_trade(channel_id, data[2])
					return 0

				#processing snapshot list
				if type(second_argument) is list:
					channel_id = data[0]
					for item in data[1]:
						self.process_trade(channel_id, item)
					return 0

			except IndexError:
				self.log.write({
					'exchange': self.exchange,
					'message': 'LIST data malformed',
					'error': traceback.format_exc(),
					'value': repr(data),
				}, 'ERROR')
				return 0

		

def listener_thread(args):

	job_key = args['job_key']
	module_config = redis.get_agent_config(job_key)

	ws = BitfinexWS({
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

def push_command_thread(job_key, commands):
	#delay writing to Redis by 10 seconds, allow WS to connect
	waiting = 1
	max_waiting = 10
	while waiting <= max_waiting:
		time.sleep(1)
		redis_key = redis.get_dict(job_key)
		if redis_key is not None:
			break
		waiting = waiting + 1

	for msg in commands:
		redis.push_msg_pool(job_key, msg.encode('utf-8'))

def push_ping_thread(job_key, command):
	#delay writing to Redis by 10 seconds, allow WS to connect
	waiting = 1
	max_waiting = 10
	while waiting <= max_waiting:
		time.sleep(1)
		redis_key = redis.get_dict(job_key)
		if redis_key is not None:
			break
		waiting = waiting + 1

	while True:
		time.sleep(1)
		redis.push_msg_pool(job_key, command.encode('utf-8'))

class BitfinexWS_start():
	def __init__(self, args):

		job_key = args['job_key']
		module_config = redis.get_agent_config(job_key)

		listener = threading.Thread(target=listener_thread, args=(args,))

		#prepare list of WS subscribe commands to be pushed to Redis
		commands = []
		markets = module_config['market'].split('_')
		streams = ''
		for market in markets:
			if (len(market) > 0):
				command = '{{"event": "subscribe", "channel": "trades", "symbol": "t{market}"}}'.format(market=market)
				commands.append(command)

		push_command = threading.Thread(
			target=push_command_thread, 
			args=(job_key, commands)
		)

		push_ping = threading.Thread(
			target=push_ping_thread, 
			args=(job_key, '{"event":"ping","cid": 1234}')
		)

		listener.start() 
		push_command.start()
		push_ping.start()

		listener.join() 
		push_command.join()
		push_ping.join()


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Bitfinex trade WS agent')
	parser.add_argument('job_key', help='Redis job key, string, required')
	parser.add_argument('container_name', help='Unique name of container, string, required')
	args = parser.parse_args()
	BitfinexWS_start({
		"job_key": args.job_key,
		"container_name": args.container_name,
	})