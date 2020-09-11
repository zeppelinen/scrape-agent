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
from syshelper import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_trades = os.environ['TOPIC_TRADES_PREFIX'] + '-' + os.environ['ENV']

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}

class BinanceCombinedWS(QAgentWS):
	def __init__(self, args):

		#parent interface init
		try:
			self.config = args['config']

			#init metrics gathering
			self.metrics = QMetrics({'job_key': 'job_trades'})
			self.counter_scraped_granular = self.metrics.create_gauge('trades_scraped_granular', 'Granular trades scraped', ['exchange', 'market'])
			self.counter_scraped_total = self.metrics.create_gauge('trades_scraped_total', 'Amount of books scraped successfully', ['exchange'])

			#populating URL for WS listener
			markets = self.config['market'].lower().split('_')
			streams = ''
			for market in markets:
				streams = streams + market+'@aggTrade/'
			
			args['ws_config'] = {}
			args['ws_config']['url'] = self.config['url'].replace('[STREAMS]', streams)
			
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
		\"https://api.binance.com/api/v1/aggTrades?symbol=[MARKET]&startTime=[STARTTIME]&endTime=[ENDTIME]&limit=1000\" \
		{starttime} {endtime} {step} {cutoff}".format(
			exchange=self.config['exchange'],
			module="binancetradesrest",
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
			stream = json.loads(msg)
			data = stream['data']
		except (ValueError, KeyError) as e:
			self.log.write({
				'exchange': self.exchange,
				'message': 'JSON data malformed',
				'error': traceback.format_exc(),
				'value': repr(msg),
			}, 'ERROR')
			return 0

		try:
			if data['m']:
				quantity = float(data['q'])
			else:
				quantity = -1 * float(data['q'])
			trade = {
				"exchange": self.exchange,
				"market": data['s'],
				"agent_id": self.r.agent_key,
				"message_id": uuid.uuid4().hex,
				"external_timestamp": datetime.datetime.utcfromtimestamp(int(data['E'])/1000).isoformat(),
				"external_id": str(data['a']),
				"price": float(data['p']),
				"quantity": quantity,
				"original_response": json.dumps(data)
			}
			self.pubsub.write(trade)
			self.metrics.gauge_inc(self.counter_scraped_granular, *[self.exchange, data['s']])
			self.r.data_scraped(1)
		except Exception as ex:
			self.log.write({
					'exchange': self.exchange,
					'message': "Binance Trades: an exception of type {name} occurred. Arguments:\n{arguments!r}".format(name=type(ex).__name__, arguments=ex.args),
					'error': traceback.format_exc(),
				}, 'ERROR')



class BinanceCombinedWS_start():
	def __init__(self, args):

		redis = QRedis(redis_config)
		job_key = args['job_key']
		module_config = redis.get_agent_config(job_key)

		ws = BinanceCombinedWS({
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

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Binance trade WS agent')
	parser.add_argument('job_key', help='Redis job key, string, required')
	parser.add_argument('container_name', help='Unique name of container, string, required')
	args = parser.parse_args()
	BinanceCombinedWS_start({
		"job_key": args.job_key,
		"container_name": args.container_name,
	})