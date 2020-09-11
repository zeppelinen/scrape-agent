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

class BitmexWS(QAgentWS):
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

		if 'data' not in data or type(data['data']) is not list:
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON datatypes missmatch',
				'error': traceback.format_exc(),
				'value': repr(data),
			}, 'ERROR')
			return 0

		if 'action' in data:
			try:
				if data['action'] == 'partial':
					self.set_snapshot(data['data'])
				if data['action'] == 'delete':
					self.delete_from_snapshot(data['data'])
				if data['action'] == 'insert':
					self.insert_into_snapshot(data['data'])
				if data['action'] == 'update':
					self.update_snapshot(data['data'])
			except Exception as ex:
				self.log.write({
					'exchange': self.exchange,
					'message': "Bitmex Order Snapshot {action}: an exception of type {name} occurred. Arguments:\n{arguments!r}".format(action=data['action'], name=type(ex).__name__, arguments=ex.args),
					'error': traceback.format_exc(),
				}, 'ERROR')
		else:
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON datatypes missmatch, "action" expected',
				'error': traceback.format_exc(),
				'value': repr(data),
			}, 'ERROR')

	def set_snapshot(self, data):
		snapshot = {}

		#print('PROCESSING THE SNAPSHOT')
		for item in data:
			if item['symbol'] in snapshot:
				snapshot[item['symbol']][item['id']] = item
			else:
				snapshot[item['symbol']] = {item['id']: item}
		#print('PROCESSING THE SNAPSHOT DONE')
		q = 0
		s = 0
		for symbol, levels in snapshot.items():
			s += 1
			q += len(levels)
		
		print('Initial snapshot takes {s} markets and {q} price levels (for all markets)'.format(s=s, q=q))
		#sys.exit()
		self._set_snapshot(snapshot)

	def _set_snapshot(self, snapshot):
		self.snapshot = snapshot

	def delete_from_snapshot(self, data):
		self.r.data_scraped(len(data))
		for item in data:
			if item['id'] not in self.snapshot[item['symbol']]:
				self.log.write({
					'exchange': self.exchange,
					'message': 'ORDERBOOK, delete ID {id} for market {market} not found'.format(id=item['id'], market=item['symbol']),
					'error': repr(item),
				}, 'ERROR')
			else:
				self.process_order(item, "DELETE")
				self.snapshot[item['symbol']].pop(item['id'])
				#reassign dict to free up memory used
				tmp = copy.deepcopy(self.snapshot[item['symbol']])
				self.snapshot[item['symbol']] = tmp
				#print('DELETED id={id}'.format(id=item['id']))


	def insert_into_snapshot(self, data):
		self.r.data_scraped(len(data))
		for item in data:
			if item['symbol'] not in self.snapshot:
				self.snapshot[item['symbol']] = {}

			self.snapshot[item['symbol']][item['id']] = item
			self.process_order(item, "INSERT")
			#print('INSERTED id={id}'.format(id=item['id']))
			

	def update_snapshot(self, data):
		self.r.data_scraped(len(data))
		for item in data:
			if item['id'] not in self.snapshot[item['symbol']]:
				self.log.write({
					'exchange': self.exchange,
					'message': 'ORDERBOOK, update ID {id} for market {market} not found'.format(id=item['id'], market=item['symbol']),
					'error': repr(item),
				}, 'ERROR')
			else:
				self.snapshot[item['symbol']][item['id']]['size'] = item['size']
				self.process_order(item, "UPDATE")
				#print('UPDATED id={id}'.format(id=item['id']))

	def get_snapshot_item(self, item):
		if item['symbol'] not in self.snapshot or item['id'] not in self.snapshot[item['symbol']]:
			self.log.write({
				'exchange': self.exchange,
				'message': 'ORDERBOOK, cannot locate item {id} for market {market} '.format(id=item['id'], market=item['symbol']),
				'error': repr(item),
			}, 'ERROR')
			return False
		else:
			return self.snapshot[item['symbol']][item['id']]

	def write(self, item):
		internal_timestamp = datetime.datetime.utcnow().isoformat()
		quantity = float(item['size'])
		price = float(item['price'])
		if (item['side'] == "Sell"):
			price = -1 * price
		order = {
			"exchange": self.exchange,
			"market": item['symbol'],
			"agent_id": self.r.agent_key,
			"message_id": uuid.uuid4().hex,
			"internal_timestamp": internal_timestamp,
			"external_id": str(item['id']),
			"price": price,
			"quantity": quantity,
			"type": "ABS",
			"original_response": json.dumps(item)
		}
		self.pubsub.write(order)

	def process_order(self, item, mode):
		s_item = self.get_snapshot_item(item)
		if s_item is not False:
			if mode == "DELETE":
				s_item['size'] = 0
				self.write(s_item)
			if mode == "UPDATE":
				self.write(s_item)
			if mode == "INSERT":
				self.write(s_item)


		

def listener_thread(config):
	ws = BitmexWS({
		"config": config,
		"log": {
			"name": "agent-log",
			"write_severity": "ALL",
		},
		"redis": redis_config,
		"topic_name": pubsub_topic_orders,
	})

def push_command_thread(agent_key, command):
	#delay writing to Redis by 10 seconds, allow WS to connect
	waiting = 1
	max_waiting = 10
	while waiting <= max_waiting:
		time.sleep(1)
		redis_key = redis.get_dict(agent_key)
		if redis_key is not None:
			break
		waiting = waiting + 1

	command = command.encode('utf-8')
	redis.push_msg_pool(agent_key, command)

class BitmexOrdersWS_start():
	def __init__(self, config):

		listener = threading.Thread(target=listener_thread, args=(config,))
		
		agent_key = redis.generate_key(config)
		command = '{"op": "subscribe", "args": ["orderBookL2"]}'
		push_command = threading.Thread(
			target=push_command_thread, 
			args=(agent_key, command)
		)

		listener.start() 
		push_command.start() 

		listener.join() 
		push_command.join()


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Bitmex orders WS agent')
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
	BitmexOrdersWS_start({
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