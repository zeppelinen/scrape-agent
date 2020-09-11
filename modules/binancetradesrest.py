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
from qagentrequest import *
from qmetrics import *
from luminati import *
from qproxy import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_trades = os.environ['TOPIC_TRADES_PREFIX'] + '-' + os.environ['ENV']

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}

class BinanceTradesREST(QAgentRequest):

	processed_urls_counter = 0
	processed_urls = []
	trades_scraped_total = 0
	
	def __init__(self, args):

		try:
			#init a proxy obj
			#self.lum = Luminati()

			self.config = args['config']
			
			#init metrics gathering
			self.metrics = QMetrics({'job_key': 'job_historical_trades'})
			self.counter_scraped_granular = self.metrics.create_counter('historical_trades_scraped_granular', 'Granular historical trades scraped', ['exchange', 'market'])
			self.counter_scraped_total = self.metrics.create_gauge('historical_trades_scraped_total', 'Amount of historical scraped successfully', ['exchange'])
			
			#parent parsing obj
			QAgentRequest.__init__(self, args)
			
		except Exception as ex:
			self.log.write({
				'exchange': self.exchange,
				'message': "An exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
				'error': traceback.format_exc(),
			}, 'ERROR')

	def push_metrics(self):
		self.metrics.gauge_set(self.counter_scraped_total, *[self.exchange], value=self.trades_scraped_total)
		self.log.write('SETTING TOTALS SCRAPED TO {totals}'.format(totals=self.trades_scraped_total), 'DEBUG')

		QAgentRequest.push_metrics(self)
		self.mysql_finish_job(self.r.get_agent_container(self.r.agent_key))
		self.r.r.delete(self.r.agent_key)

	#callback from Queue consumers happens here
	def parse_url(self, url, callback):
		#proxy = self.lum.get_new_session()
		proxy = self.r.get_random_proxy(self.exchange)
		#print("SET NEW PROXY {proxy}".format(proxy=proxy))
		self.set_proxy(proxy)
		response = self.http_get(url['url'])
		if (response['result']):
			#print("RESPONSE {http_code} from {url} at {time}".format(http_code=response['http_code'], url=url['url'], time=datetime.datetime.utcnow().isoformat()))
			if (int(response['http_code']) == 200):
				if (self.cut_off()):
					pass
				else:
					parse_result = self.process_trades(response['json'], url)
					self.log.write("PROCESSED {trades} TRADES from {url}".format(trades=len(response['json']), url=url['url']), 'DEBUG')
					#paginate further in time if limit is hit
					if (parse_result['trades'] == 1000):
						url['endtime'] = parse_result['first_timestamp'] - 1
						url['url'] = self.config['url'].replace('[MARKET]', url['market']).replace('[STARTTIME]', str(url['starttime'])).replace('[ENDTIME]', str(url['endtime']))
						self.log.write("PAGINATE NEW URL {url}".format(url=url['url']), 'DEBUG')
						self.put_in_queue(url)
			else:
				self.retry(response, url)

		else:
			self.retry(response, url)
		callback()

	def retry(self, response, url):
		#print(response)
		if 'http_code' in response:
			error = response['http_code']
		else:
			error = response['error']
		self.log.write('ERROR: {error} from {url} at {time}'.format(error=error, url=url, time=datetime.datetime.utcnow().isoformat()), 'ERROR')
		if not self.cut_off():
			#put url in the queue again, increment attempt
			self.put_in_queue(url)
		

	def process_trades(self, trades, url):
		self.processed_urls_counter += 1
		self.processed_urls.append(url['url'])
		first_timestamp = url['endtime']
		for data in trades:
			if int(data['T']) < first_timestamp:
				first_timestamp = int(data['T'])
			if data['m']:
				quantity = float(data['q'])
			else:
				quantity = -1 * float(data['q'])
			trade = {
				"exchange": self.exchange,
				"market": url['market'],
				"agent_id": self.r.agent_key,
				"message_id": uuid.uuid4().hex,
				"external_timestamp": datetime.datetime.utcfromtimestamp(int(data['T'])/1000).isoformat(),
				"external_id": str(data['f']),
				"price": float(data['p']),
				"quantity": quantity,
				"original_response": json.dumps(data)
			}
			#print(trade)
			self.pubsub.write(trade)
			self.trades_scraped_total += 1
			self.metrics.counter_inc(self.counter_scraped_granular, *[self.exchange, url['market']])
		return {
			'trades': len(trades),
			'first_timestamp': first_timestamp
		}
		


class BinanceTradesREST_start():
	def __init__(self, args):

		print("STARTING: at {time}".format(time=datetime.datetime.utcnow().isoformat()))

		redis = QRedis(redis_config)
		job_key = args['job_key']
		module_config = redis.get_agent_config(job_key)

		#limit of time range is 1 hr for Binance
		time_range_limit = 3600*1000

		#make time periods lower than time_range_limit
		time_ranges = []
		module_config['starttime'] = int(module_config['starttime'])
		module_config['endtime'] = int(module_config['endtime'])
		if (module_config['endtime'] - module_config['starttime']) < time_range_limit:
			time_ranges.append([module_config['starttime'], module_config['endtime']])
		else:
			start = module_config['starttime']
			end = module_config['endtime']
			while start + time_range_limit < end:
				time_ranges.append([start, start + time_range_limit])
				start += (time_range_limit + 1)
			time_ranges.append([start, end])

		markets = module_config['market'].upper().split('_')
		urls_list = []
		for market in markets:
			for ranges in time_ranges:
				url = module_config['url'].replace('[MARKET]', market).replace('[STARTTIME]', str(ranges[0])).replace('[ENDTIME]', str(ranges[1]))
				urls_list.append({
					'market': market,
					'url': url,
					'starttime': ranges[0],
					'endtime': ranges[1]
				})

		#redefine cut off window for longer jobs
		module_config['cut_off_window'] = int(module_config['cut_off_window'])

		ws = BinanceTradesREST({
			"urls_list": urls_list,
			"config": module_config,
			"container_name": args['container_name'],
			"log": {
				"name": "agent-log",
				"write_severity": "ALL",
			},
			"redis": redis_config,
			"topic_name": pubsub_topic_trades,
			"disable_cut_off_report": True
		})

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Binance order REST agent')
	parser.add_argument('job_key', help='Redis job key, string, required')
	parser.add_argument('container_name', help='Unique name of container, string, required')
	args = parser.parse_args()
	BinanceTradesREST_start({
		"job_key": args.job_key,
		"container_name": args.container_name
	})