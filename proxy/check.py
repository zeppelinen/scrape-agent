import os
import sys
import time
import traceback
import logging as lg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from threading import Thread
from queue import Queue
import random
import json

sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))

from gclog import *
from qmetrics import *
from qmysql import *
from qredis import *
from qrequest import *

try:
	import asyncio
except ImportError:
	import trollius as asyncio

gclog = GCLog({
	"name": "proxy-monitor-log",
	"write_severity": "INFO",
})

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
redis = QRedis({
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
})

class ProxyMonitor():
	
	num_fetch_threads = 30
	
	def __init__(self):
		self.db = QMySql()
		self.init_queues()
		self.init_metrics()
		gclog.write('[SCHEDULER ALIVE]', 'INFO')
		self.run_proxy_monitor()

	def init_metrics(self):
		self.metrics = QMetrics({'job_key': 'job_proxy'})
		self.proxy_counter = self.metrics.create_gauge('proxy_status', 'Number of active proxy per exchange', ['exchange'])

	def push_metrics(self):
		job_key = self.metrics.push_metrics()
		gclog.write('PUSHED SOME METRICS', 'DEBUG')
		redis.append_pg_key(job_key)

	def put_in_queue_proxy(self, item):
		self.queue_proxy.put(item)

	def task_done_proxy(self):
		self.queue_proxy.task_done()

	def put_in_queue_result(self, item):
		self.queue_result.put(item)

	def task_done_result(self):
		self.queue_result.task_done()

	def init_queues(self):
		self.queue_proxy = Queue()
		for i in range(self.num_fetch_threads):
			worker = Thread(target=self.consume_proxies, args=(i,))
			worker.setDaemon(True)
			worker.start()

		self.queue_result = Queue()
		worker = Thread(target=self.consume_results, args=())
		worker.setDaemon(True)
		worker.start()

	def consume_proxies(self, i):
		while True:
			#print('{i}: LOOKING FOR NEXT URL'.format(i=i))
			item = self.queue_proxy.get()
			result = self.is_bad_proxy(item, self.task_done_proxy)
			q_result = item
			q_result['result'] = result
			self.put_in_queue_result(q_result)

	def is_bad_proxy(self, item, callback):
		gclog.write('TESTING {proxy_host_port} AGAINST {url} ...'.format(proxy_host_port=item['proxy_host_port'], url=item['url']), 'DEBUG')
		proxy = {
			'host_port': item['proxy_host_port'],
			'user': item['proxy_user'],
			'password': item['proxy_password']
		}
		req = Qrequest({'proxy': proxy})
		if item['headers'] is not None:
			req.set_headers(json.loads(item['headers']))
		result = req.http_get(item['url'])
		callback()
		return result

	def consume_results(self):
		while True:
			try:
				item = self.queue_result.get()
				self.replace_proxy_results(item)
				self.task_done_result()
			except Exception as ex:
				gclog.write({
					'message': "[PROXY MONITOR DEAD]: Scheduler exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
					'error': traceback.format_exc(),
				}, 'ERROR')
				os._exit(1)

	#monitor all proxies against all websites, write check results to DB
	def run_proxy_monitor(self):
		while True:
			try:
				self.db.connect()
				#getting number of current running/pending jobs
				proxies = self.db.fetchall("SELECT * FROM `proxy_list`")
				websites = self.db.fetchall("SELECT * FROM `proxy_website` WHERE `enabled` = 1")
				gclog.write('STATUS: {proxies} PROXIES BEING TESTED AGAINST {websites} WEBSITES'.format(proxies=len(proxies), websites=len(websites)), 'INFO')
				checks_list = []
				for website in websites:
					self.remove_unsync_proxies(website[1], proxies)

					random_proxy = redis.get_random_proxy(website[1])
					gclog.write('GOT {random_proxy} PROXY FOR {website} WEBSITE'.format(random_proxy=random_proxy, website=website), 'INFO')
					for proxy in proxies:
						website_id = website[0]
						url = website[3]
						headers = website[4]
						proxy_id = proxy[0]
						proxy_host_port = "{host}:{port}".format(host=proxy[1], port=proxy[2])
						proxy_user = proxy[3]
						proxy_password = proxy[4]
						proxy_weight = proxy[5]
						q_proxy = {
							'website': website[1],
							'website_id': website_id,
							'url': url,
							'headers': headers,
							'proxy_id': proxy_id,
							'proxy_host_port': proxy_host_port,
							'proxy_user': proxy_user,
							'proxy_password': proxy_password,
							'proxy_weight': proxy_weight
						}
						checks_list.append(q_proxy)
				
				#shuffle all the checks first
				random.shuffle(checks_list)
				for item in checks_list:
					self.put_in_queue_proxy(item)

				self.db.commit()
				self.db.close()
				while self.queue_proxy.qsize() > 10:
					for website in websites:
						proxy_counter = redis.get_hash_len(redis.make_proxy_key(website[1]))
						self.metrics.gauge_set(self.proxy_counter, *[website[1]], value=proxy_counter)
					self.push_metrics()
					gclog.write('WAITING FOR QUEUE ({proxy_len}) TO BE SHORTER'.format(proxy_len=self.queue_proxy.qsize()), 'INFO')
					time.sleep(5)
			except Exception as ex:
				gclog.write({
					'message': "[PROXY MONITOR DEAD]: Scheduler exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
					'error': traceback.format_exc(),
				}, 'ERROR')
				os._exit(1)

	#in case proxy was removed from storage it has to be removed from cached list
	def remove_unsync_proxies(self, website, db_proxies):
		json_proxies = []
		for db_proxy in db_proxies:
			proxy = {
				'host_port': "{host}:{port}".format(host=db_proxy[1], port=db_proxy[2]),
				'user': db_proxy[3],
				'password': db_proxy[4]
			}
			encoded = json.dumps(proxy)
			json_proxies.append(encoded)
		cached_proxies = redis.get_dict(redis.make_proxy_key(website))
		for cached_proxy in cached_proxies:
			cached_proxy = cached_proxy.decode('utf8')
			if cached_proxy not in json_proxies:
				print('REMOVING {cached_proxy} FROM {website} LIST'.format(cached_proxy=cached_proxy, website=website))
				redis.del_proxy_key(website, json.loads(cached_proxy))

	def replace_proxy_results(self, item):
		proxy = {
			'host_port': item['proxy_host_port'],
			'user': item['proxy_user'],
			'password': item['proxy_password']
		}
		if item['result']['result'] and item['result']['http_code'] == 200:
			redis.append_proxy_key(item['website'], proxy, item['proxy_weight'])
			status = 'OK'
			gclog.write('OK, HTTP_CODE={http_code}, proxy={proxy}'.format(http_code=item['result']['http_code'], proxy=proxy), 'DEBUG')
		else:
			redis.del_proxy_key(item['website'], proxy)
			status = 'ERROR'
			if 'error' in item['result']:
				error = item['result']['error']
			else:
				error = item['result']['http_code']
			gclog.write('ERROR, ERROR={error}, proxy={proxy}'.format(error=error, proxy=proxy), 'DEBUG')

if __name__ == '__main__':

	def main():

		# Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
		try:
			pm = ProxyMonitor()
		except (KeyboardInterrupt, SystemExit):
			pass
		except Exception as ex:
			gclog.write({
				'message': "[PROXY MONITOR DEAD]: Scheduler exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
				'error': traceback.format_exc(),
			}, 'ERROR')
			os._exit(1)

	try:
		main()
	except Exception as ex:
		gclog.write({
			'message': "[PROXY MONITOR DEAD]: Scheduler exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
			'error': traceback.format_exc(),
		}, 'ERROR')
		os._exit(1)