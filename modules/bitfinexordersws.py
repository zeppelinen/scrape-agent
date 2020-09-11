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
from qredis import *
import copy

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_orders = os.environ['TOPIC_ORDERS_PREFIX'] + '-' + os.environ['ENV']

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}
redis = QRedis(redis_config)

class BitfinexOrdersWS(QAgentWS):

	channel_ids = {}
	snapshot = {}

	max_waiting_time = 0.2
	max_batch_size = 300

	def __init__(self, args):

		#parent interface init
		try:
			#init metrics gathering
			self.metrics = QMetrics({'job_key': 'job_orders'})
			self.counter_scraped_total = self.metrics.create_gauge('orders_scraped_total', 'Amount of orders scraped successfully', ['exchange'])

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

		#reset accumulate counter
		self.r.data_scrape_counter_reset()

	def get_market_by_channel_id(self, channel_id):
		if channel_id not in self.channel_ids:
			self.log.write({
				'exchange': self.exchange,
				'message': 'Unknown channel id: '+channel_id,
				'error': traceback.format_exc(),
			}, 'ERROR')
		else:
			return self.channel_ids[channel_id]

	def msg_received(self, msg):
		QAgentWS.msg_received(self, msg)
		if "{" in msg:
			self.process_json_obj(msg)
		#the rest goes to queue
		else:
			self.process_json_list(json.loads(msg))
		return 0

	def process_json_obj(self, msg):
		try:
			data = json.loads(msg)
		except ValueError:
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON data malformed',
				'error': traceback.format_exc(),
				'value': repr(msg),
			}, 'ERROR')
		#processing WS info messages
		if 'event' in data:
			event = data['event']
			#skip intro message
			if event == 'info':
				print('SKIP INTRO MESSAGE')
				return 0

			if event == 'pong':
				#print('PONG MESSAGE RECEIVED')
				return 0

			#map channel id and market
			if event == 'subscribed':
				self.channel_ids[data['chanId']] = data['pair']
				print('SUBSCRIBED TO CHANNEL {market}'.format(market=data['pair']))
				#print(self.channel_ids)
				return 0

	def process_json_list(self, data):
		try:
			channel_id = data[0]
			second_argument = data[1]

			#skip heartbeat message
			if second_argument == "hb":
				#print('HEARTBEAT RECEIVED')
				return 0

			data_is_list = type(second_argument) is list
			if (data_is_list):
				data_is_list_of_lists = type(second_argument[0]) is list

				channel_id = data[0]

				#processing order snapshot list
				#second arg should be list of lists
				if data_is_list_of_lists:
					self.process_snapshot(channel_id, second_argument)
					return 0
				#process individual update messages
				else:
					self.process_order(channel_id, second_argument)
					return 0

		except IndexError:
			self.log.write({
				'exchange': self.exchange,
				'message': 'LIST data malformed',
				'error': traceback.format_exc(),
				'value': repr(data),
			}, 'ERROR')
			return 0

	def process_snapshot(self, channel_id, snapshot):
		s = {}
		for item in snapshot:
			s[item[0]] = [item[1], item[2]]
		self.snapshot[self.get_market_by_channel_id(channel_id)] = s

	def process_order(self, channel_id, data):
		order_id = data[0]
		order_price = data[1]
		order_quantity = data[2]
		market = self.get_market_by_channel_id(channel_id)
		#removing order if price == 0
		if (order_price == 0):
			del_order = self.snapshot[market][order_id]
			del_price = del_order[0]
			del_quantity = del_order[1]
			#convert negative quantity to negative price
			if (del_quantity < 0):
				del_price = -1 * del_price
			self.write(market, [order_id, del_price, 0])#delete level quantity is always 0
			self.snapshot[market].pop(order_id)
			#reassign deep copy to free up used memory after pop
			tmp = copy.deepcopy(self.snapshot[market])
			self.snapshot[market] = tmp
		else:
			self.snapshot[market][order_id] = [order_price, order_quantity]
			self.write(market, data)

	def write(self, market, item):
		internal_timestamp = datetime.datetime.utcnow().isoformat()
		external_id = str(item[0])
		price = float(item[1])
		quantity = float(item[2])
		#if quantity < 0 then it's ask, else - bid
		#therefore we convert quantity negative to price negative according to internal standarts
		if (quantity < 0):
			quantity = -1 * quantity
			price = -1 * price
		order = {
			"exchange": self.exchange,
			"market": market,
			"agent_id": self.r.agent_key,
			"message_id": uuid.uuid4().hex,
			"internal_timestamp": internal_timestamp,
			"external_id": external_id,
			"price": price,
			"quantity": quantity,
			"type": "ABS",
			"original_response": json.dumps(item)
		}
		self.r.data_scraped(1)
		self.pubsub.write(order)

		

def listener_thread(config):
	ws = BitfinexOrdersWS({
		"config": config,
		"log": {
			"name": "agent-log",
			"write_severity": "ALL",
		},
		"redis": redis_config,
		"topic_name": pubsub_topic_orders,
	})

def push_command_thread(agent_key, commands):
	#delay writing to Redis by 10 seconds, allow WS to connect
	waiting = 1
	max_waiting = 10
	while waiting <= max_waiting:
		time.sleep(1)
		redis_key = redis.get_dict(agent_key)
		if redis_key is not None:
			break
		waiting = waiting + 1

	for msg in commands:
		redis.push_msg_pool(agent_key, msg.encode('utf-8'))

def push_ping_thread(agent_key, command):
	#delay writing to Redis by 10 seconds, allow WS to connect
	waiting = 1
	max_waiting = 10
	while waiting <= max_waiting:
		time.sleep(1)
		redis_key = redis.get_dict(agent_key)
		if redis_key is not None:
			break
		waiting = waiting + 1

	while True:
		time.sleep(5)
		redis.push_msg_pool(agent_key, command.encode('utf-8'))

class BitfinexOrdersWS_start():
	def __init__(self, config):

		listener = threading.Thread(target=listener_thread, args=(config,))

		#prepare list of WS subscribe commands to be pushed to Redis
		commands = []
		markets = config['market'].split('_')
		streams = ''
		for market in markets:
			if (len(market) > 0):
				command = '{{"event": "subscribe", "channel": "book", "prec": "R0", "len": 100, "freq": "F0", "symbol": "t{market}"}}'.format(market=market)
				commands.append(command)

		agent_key = redis.generate_key(config)
		push_command = threading.Thread(
			target=push_command_thread, 
			args=(agent_key, commands)
		)

		#push_ping = threading.Thread(
		#	target=push_ping_thread, 
		#	args=(agent_key, '{"event":"ping","cid": 1234}')
		#)

		listener.start() 
		push_command.start()
		#push_ping.start()

		listener.join() 
		push_command.join()
		#push_ping.join()


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Bitfinex trade WS agent')
	parser.add_argument('exchange', help='Exchange name, string, required')
	parser.add_argument('data', help='Target data (e.g. ORDERS, TRADES), string, required')
	parser.add_argument('url', help='Target URL (e.g. wss://websockets.com:1234), string, required')
	parser.add_argument('market', help='Target MARKET (e.g. BNBBTC), string, required')
	parser.add_argument('module', help='Python agent module, string, required')
	parser.add_argument('schedule', help='Schedule start setting, string, required')
	parser.add_argument('restart', help='Schedule Restart setting, string, required')
	parser.add_argument('restart_after', help='Restart after (seconds), string, required')
	parser.add_argument('copy', help='# of Agent copy running, int, required')
	parser.add_argument('container_name', help='Unique name of container, string, required')
	args = parser.parse_args()
	BitfinexOrdersWS_start({
		"exchange": args.exchange,
		"data": args.data,
		"url": args.url,
		"market": args.market,
		"module": args.module,
		"schedule": args.schedule,
		"restart": args.restart,
		"restart_after": args.restart_after,
		"copy": args.copy,
		"container_name": args.container_name,
	})