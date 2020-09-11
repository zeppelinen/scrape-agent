import json
import threading
import sys
import os
import datetime
import traceback
import argparse
import uuid
from hashlib import sha256
from cerberus import Validator
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from qagentws import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_orders = os.environ['TOPIC_ORDERS_PREFIX'] + '-' + os.environ['ENV']

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}

class BinanceCombinedOrdersWS(QAgentWS):

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
		except (ValueError, KeyError) as e:
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON data malformed',
				'error': traceback.format_exc(),
				'value': repr(msg),
			}, 'ERROR')
			return 0

		try:
			market = data['data']['s']
			external_timestamp = datetime.datetime.utcfromtimestamp(int(data['data']['E'])/1000).isoformat()
			internal_timestamp = datetime.datetime.utcnow().isoformat()
			
			self.r.data_scraped(len(data['data']['b'])+len(data['data']['a']))
			for item in data['data']['b']:
				bid = {
					"exchange": self.exchange,
					"market": market,
					"agent_id": self.r.agent_key,
					"message_id": uuid.uuid4().hex,
					"external_timestamp": external_timestamp,
					"internal_timestamp": internal_timestamp,
					"price": float(item[0]),
					"quantity": float(item[1]),
					"type": "ABS",
					"original_response": json.dumps(item)
				}
				self.pubsub.write(bid)

			for item in data['data']['a']:
				ask = {
					"exchange": self.exchange,
					"market": market,
					"agent_id": self.r.agent_key,
					"message_id": uuid.uuid4().hex,
					"external_timestamp": external_timestamp,
					"internal_timestamp": internal_timestamp,
					"price": -1 * float(item[0]),
					"quantity": float(item[1]),
					"type": "ABS",
					"original_response": json.dumps(item)
				}
				self.pubsub.write(ask)
		except Exception as ex:
			self.log.write({
					'exchange': self.exchange,
					'message': "Binance Orders: an exception of type {name} occurred. Arguments:\n{arguments!r}".format(name=type(ex).__name__, arguments=ex.args),
					'error': traceback.format_exc(),
				}, 'ERROR')
		



class BinanceCombinedOrdersWS_start():
	def __init__(self, config):

		markets = config['market'].lower().split('_')
		streams = ''
		for market in markets:
			streams = streams + market+'@depth/'
		
		config['url'] = config['url'].replace('[STREAMS]', streams)

		ws = BinanceCombinedOrdersWS({
			"config": config,
			"log": {
				"name": "agent-log",
				"write_severity": "ALL",
			},
			"redis": redis_config,
			"topic_name": pubsub_topic_orders,
		})

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Binance order WS agent')
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
	BinanceCombinedOrdersWS_start({
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