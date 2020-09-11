import json
import threading
import sys
import os
import datetime
import traceback
import argparse
import uuid
import base64
from cerberus import Validator
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from qagentmisc import *
from syshelper import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_misc = os.environ['TOPIC_MISC_PREFIX'] + '-' + os.environ['ENV']

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}

class MiscBitmexStats(QAgentMisc):
	def __init__(self, args):

		#parent interface init
		try:
			self.config = args['config']

			#init metrics gathering
			self.metrics = QMetrics({'job_key': 'job_misc_bitmex_stats'})
			self.counter_scraped_total = self.metrics.create_gauge('misc_bitmex_stats_total', 'Amount of OPEN_VALUE and OPEN_INTEREST scraped successfully', ['exchange'])

			QAgentMisc.__init__(self, args)
		except Exception as ex:
			self.log.write({
				'exchange': self.exchange,
				'message': "An exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
				'error': traceback.format_exc(),
			}, 'ERROR')

	def pull_data(self):
		self.log.write('[PULL DATA]: ADDING URL IN A QUEUE', 'DEBUG')
		self.put_in_queue(self.config['url'])

	#callback from Queue consumers happens here
	def parse_url(self, url, callback):
		#proxy = self.lum.get_new_session()
		proxy = self.r.get_random_proxy(self.exchange)
		#print("SET NEW PROXY {proxy}".format(proxy=proxy))
		self.set_proxy(proxy)
		response = self.http_get(url)
		if (response['result']):
			print("RESPONSE {http_code} from {url} at {time}".format(http_code=response['http_code'], url=url, time=datetime.datetime.utcnow().isoformat()))
			if (int(response['http_code']) == 200):
				self.process_snapshot(response['json'], url)
			else:
				self.retry(response, url)

		else:
			self.retry(response, url)
		callback()

	def process_snapshot(self, response, url):
		internal_timestamp = datetime.datetime.utcnow().isoformat()
		for item in response:
			misc_open_interest = {
				"type": "OPEN_INTEREST",
				"source": self.exchange,
				"identifier": item['rootSymbol'],
				"exchange": self.exchange,
				"asset_code": item['rootSymbol'],
				"internal_timestamp": internal_timestamp,
				"value": item['openInterest'],
				"data": json.dumps(item)
			}
			self.pubsub.write(misc_open_interest)

			misc_open_value = {
				"type": "OPEN_VALUE",
				"source": self.exchange,
				"identifier": item['rootSymbol'],
				"exchange": self.exchange,
				"asset_code": item['rootSymbol'],
				"internal_timestamp": internal_timestamp,
				"value": item['openValue'],
				"data": json.dumps(item)
			}
			self.pubsub.write(misc_open_value)
			#tracking data scraped
			self.r.data_scraped(2)
			self.r.upd_msg_uptime()


	def retry(self, response, url):
		#print(response)
		if 'http_code' in response:
			error = response['http_code']
		else:
			error = response['error']
		self.log.write('ERROR: {error} from {url} at {time}'.format(error=error, url=url, time=datetime.datetime.utcnow().isoformat()), 'ERROR')
		self.put_in_queue(url)

	def push_metrics(self):
		self.metrics.gauge_set(self.counter_scraped_total, *[self.exchange], value=self.r.data_accumulate_counter)
		
		job_key = self.metrics.push_metrics()
		print('PUSHED SOME METRICS')
		self.r.append_pg_key(job_key)

		#reset accumulate counter
		self.r.data_scrape_counter_reset()




class MiscBitmexStats_start():
	def __init__(self, args):

		redis = QRedis(redis_config)
		job_key = args['job_key']
		module_config = redis.get_agent_config(job_key)


		ws = MiscBitmexStats({
			"config": module_config,
			"container_name": args['container_name'],
			"log": {
				"name": "agent-log",
				"write_severity": "ALL",
			},
			"redis": redis_config,
			"topic_name": pubsub_topic_misc
		})

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Bitmex MISC agent')
	parser.add_argument('job_key', help='Redis job key, string, required')
	parser.add_argument('container_name', help='Unique name of container, string, required')
	args = parser.parse_args()
	MiscBitmexStats_start({
		"job_key": args.job_key,
		"container_name": args.container_name,
	})