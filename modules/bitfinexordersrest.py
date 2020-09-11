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
from luminati import *

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
pubsub_topic_books = os.environ['TOPIC_BOOKS_PREFIX'] + '-' + os.environ['ENV']

redis_config = {
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
}

class BitfinexOrdersREST(QAgentRequest):
	
	def __init__(self, args):

		try:
			
			#init metrics gathering
			self.metrics = QMetrics({'job_key': 'job_books'})
			self.counter_books_scraped_granular = self.metrics.create_counter('books_scraped_granular', 'Amount of books scraped', ['exchange', 'market'])
			self.counter_job_last_uptime = self.metrics.create_gauge('books_uptime', 'Last uptime since books were scraped', ['exchange'])
			self.counter_data_published = self.metrics.create_counter('books_published', 'Amount of books published to pubsub', ['exchange'])
			self.counter_data_scraped_total = self.metrics.create_counter('books_data_scraped_total', 'Amount of books scraped successfully', ['exchange'])
			self.counter_job_finished = self.metrics.create_counter('books_jobs_finished', 'Amount of books jobs finished successfully', ['exchange'])

			#parent parsing obj
			QAgentRequest.__init__(self, args)
			
		except Exception as ex:
			self.log.write({
				'exchange': self.exchange,
				'message': "An exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
				'error': traceback.format_exc(),
			}, 'ERROR')

	def push_metrics(self):
		#set the last uptime before sending metrics
		self.metrics.set_gauge_to_now(self.counter_job_last_uptime, *[self.exchange])
		#set the amount of items queued to pubsub (therefore scraped successfully)
		self.metrics.counter_inc(self.counter_data_scraped_total, *[self.exchange], increment=self.pubsub.total_queued)
		#set the amount of items published in pubsub
		self.metrics.counter_inc(self.counter_data_published, *[self.exchange], increment=self.pubsub.total_published)
		#set the job counter to +1
		self.metrics.counter_inc(self.counter_job_finished, *[self.exchange])
		
		QAgentRequest.push_metrics(self)

	#callback from Queue consumers happens here
	def parse_url(self, url, callback):
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
					self.process_snapshot(response['json'], url)
					#print("PROCESS SNAPSHOT from {url}".format(url=url['url']))
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
		

	def process_snapshot(self, data, url):
		#skip empty lists
		if (len(data['bids'])+len(data['asks']) == 0):
			print('SKIPPING EMPTY LIST')
			return
		internal_timestamp = datetime.datetime.utcnow().isoformat()
		snapshot = {
			"exchange": self.exchange,
			"market": url['market'],
			"agent_id": self.r.agent_key,
			"message_id": uuid.uuid4().hex,
			"internal_timestamp": internal_timestamp,
			"snapshot": [],
			"original_response": json.dumps(data)
		}
		for item in data['bids']:
			order = [float(item['price']), float(item['amount'])]
			snapshot['snapshot'].append(order)
		for item in data['asks']:
			order = [float(item['price']), -1 * float(item['amount'])]
			snapshot['snapshot'].append(order)

		self.pubsub.write(snapshot)
		self.metrics.counter_inc(self.counter_books_scraped_granular, *[self.exchange, url['market']])
		return
		


class BitfinexOrdersREST_start():
	def __init__(self, args):

		print("STARTING: at {time}".format(time=datetime.datetime.utcnow().isoformat()))

		redis = QRedis(redis_config)
		job_key = args['job_key']
		module_config = redis.get_agent_config(job_key)

		markets = module_config['market'].upper().split('_')
		urls_list = []
		for market in markets:
			urls_list.append({
				'market': market,
				'url': module_config['url'].replace('[MARKET]', market.lower())
			})

		ws = BitfinexOrdersREST({
			"config": module_config,
			"container_name": args['container_name'],
			"urls_list": urls_list,
			"log": {
				"name": "agent-log",
				"write_severity": "ALL",
			},
			"redis": redis_config,
			"topic_name": pubsub_topic_books,
		})

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Bitfinex order REST agent')
	parser.add_argument('job_key', help='Redis job key, string, required')
	parser.add_argument('container_name', help='Unique name of container, string, required')
	args = parser.parse_args()
	BitfinexOrdersREST_start({
		"job_key": args.job_key,
		"container_name": args.container_name
	})