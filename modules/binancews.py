import json
import threading
import sys
import os
import datetime
import traceback
import argparse
import uuid
from cerberus import Validator
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from qagentws import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_trades = os.environ['TOPIC_TRADES_PREFIX'] + '-' + os.environ['ENV']#'trades-qfx-data-dev'


class BinanceWS(QAgentWS): 
	def __init__(self, args):

		#parent interface init
		try:
			QAgentWS.__init__(self, args)
		except Exception:
			self.log.write({
				'exchange': self.exchange,
				'message': 'Unknown exception',
				'error': traceback.format_exc(),
			}, 'ERROR')

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

		v_schema = {
			'E': {'type': 'integer'},
			's': {'type': 'string'},
			'q': {'type': 'string'},
			't': {'type': 'integer'},
			'p': {'type': 'string'},
		}
		v = Validator()
		v.allow_unknown = True
		if not v.validate(data, v_schema):
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON datatypes missmatch',
				'error': repr(v.errors),
			}, 'ERROR')
			return 0
		
		trade = {
			"exchange": self.exchange,
			"market": data['s'],
			"external_timestamp": datetime.datetime.utcfromtimestamp(int(data['E'])/1000).isoformat(),
			"external_id": str(data['t']),
			"price": data['p'],
			"quantity": data['q'],
			"original_response": data
		}
		self.pubsub.write(trade, pubsub_topic_trades)



class BinanceWS_start():
	def __init__(self, args):
		
		args['url'] = args['url'].replace('[MARKET]', args['market'].lower())

		uid = uuid.uuid4().hex
		args['unique_id'] = uid
		
		ws = BinanceWS({
			"unique_id": args['unique_id'],
			"exchange": args['exchange'],
			"scheduler": {
				"schedule": args['schedule'],
				"restart": args['restart'],
				"restart_after": args['restart_after'],
			},
			"log": {
				"name": "agent-log",
				"write_severity": "ALL",
			},
			"redis": {
				"host": "redis",
				"port": 6379,
				"password": REDIS_PASSWORD
			},
			"url": args['url'],
			"verbose": 0,
			"proxy": False,
			"origin": False,
			"subprotocols": False,
			"nocert": False,
			"headers": False,
			"raw": False,
			"text": False,
			"timings": False
		})

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Binance trade WS agent')
	parser.add_argument('exchange', help='Exchange name, string, required')
	parser.add_argument('url', help='Target URL (e.g. wss://websockets.com:1234), string, required')
	parser.add_argument('market', help='Target MARKET (e.g. BNBBTC), string, required')
	args = parser.parse_args()
	BinanceWS_start({
		"exchange": args.exchange,
		"module": "BinanceWS",
		"market": args.market,
		"schedule": "instant",
		"restart": "always",
		"restart_after": 60,
		"url": args.url,
	})