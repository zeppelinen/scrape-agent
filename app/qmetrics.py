import os
import random
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway, Counter
import requests
import datetime

class QMetrics():

	job_key = 'unknown_metrics_key'
	counters = {}
	gauges = {}

	def __init__(self, args={}):
		if 'job_key' in args:
			self.job_key = args['job_key']
		self.init_pushgateway_url()
		self.registry = CollectorRegistry()

	def create_gauge(self, metrics_key, metrics_name, data_keys):
		index = len(self.gauges) + 1
		self.gauges[index] = Gauge(metrics_key, metrics_name, data_keys, registry=self.registry)
		return index

	def reinit_gauge(self, index, metrics_key, metrics_name, data_keys):
		self.registry.unregister(self.gauges[index])
		self.gauges[index] = Gauge(metrics_key, metrics_name, data_keys, registry=self.registry)
		return index

	def set_gauge_to_now(self, index, *labels):
		now = datetime.datetime.utcnow().timestamp()
		#print("SETTING GAUGE {index} TO {now}".format(index=index, now=now))
		self.gauges[index].labels(*labels).set(now)

	def gauge_set(self, index, *labels, value):
		#print("SETTING GAUGE {index} TO {value}".format(index=index, value=value))
		self.gauges[index].labels(*labels).set(value)

	def create_counter(self, metrics_key, metrics_name, data_keys):
		index = len(self.counters) + 1
		self.counters[index] = Counter(metrics_key, metrics_name, data_keys, registry=self.registry)
		return index

	def init_pushgateway_url(self):
		self.pushgateway_url = '{host}:{port}'.format(host=os.environ['PUSHGATEWAY_HOST'], port=os.environ['PUSHGATEWAY_PORT'])

	def counter_inc(self, index, *labels, increment=1):
		#print("INCREMENTING COUNTER {index} TO {increment}".format(index=index, increment=increment))
		self.counters[index].labels(*labels).inc(increment)

	def gauge_inc(self, index, *labels, increment=1):
		#print("INCREMENTING GAUGE {index} TO {increment}".format(index=index, increment=increment))
		self.gauges[index].labels(*labels).inc(increment)

	def push_metrics(self):
		job = '{job_key}_{random}'.format(job_key=self.job_key, random=random.randint(1000000000000,100000000000000000))
		push_to_gateway(self.pushgateway_url, job=job, registry=self.registry)
		return job

	def delete(self, job_key):
		r = requests.delete('http://{pushgateway_url}/metrics/job/{job_key}'.format(pushgateway_url=self.pushgateway_url, job_key=job_key))
		return r