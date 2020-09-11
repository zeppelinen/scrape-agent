import redis
from redis.sentinel import Sentinel
import datetime
import dateutil.parser
import json
import sys
import random
from hashlib import sha256
from collections import OrderedDict

SCHEDULE_CONTINUOUS = 'continuous'
SCHEDULE_CRON = 'cron'
SCHEDULE_ONE_TIME = 'onetime'
RESTART_ALWAYS = 'always'
PENDING = 'PENDING'
STARTING = 'STARTING'
RUNNING = 'RUNNING'
RESTARTING = 'RESTARTING'

class QRedis():

	job_prefix = 'JOB'
	agent_key = False
	data_per_last_min = {}
	data_accumulate_counter = 0
	pg_keys_list = 'PUSHGATEWAY_KEYS'

	def __init__(self, conf):
		self.r = redis.Redis(host=conf['host'], port=conf['port'], password=conf['password'])
		self.last_update_at = datetime.datetime.utcnow().timestamp()*1000000

	def set(self, key, value):
		return self.r.set(key, value)

	def set_dict(self, key, value):
		return self.r.hmset(key, value)

	def set_dict_field(self, key, field, value):
		return self.r.hset(key, field, value)

	def get_dict_field(self, key, field):
		value = self.r.hget(key, field)
		if value is not None:
			value = value.decode('utf-8')
		return value

	def get(self, key):
		return self.r.get(key)

	def get_dict(self, key):
		return self.r.hgetall(key)

	def del_dict_key(self, name, key):
		return self.r.hdel(name, key)

	def get_hash_len(self, name):
		return self.r.hlen(name)

	def set_agent_container(self, agent_key, container_name):
		return self.set_dict_field(agent_key, 'container_name', container_name)

	def get_agent_container(self, agent_key):
		return self.get_dict_field(agent_key, 'container_name')

	def get_agent_config(self, agent_key):
		return json.loads(self.get_dict_field(agent_key, 'config'))

	def agent_exists(self, key):
		return self.r.exists(key)

	def assign_agent_key(self, key):
		self.agent_key = key

	def init_key(self, module_conf):
		key = self.generate_key(module_conf)
		self.assign_agent_key(key)
		if (self.r.exists(self.agent_key)):
			self.respawn_agent_key(self.agent_key, module_conf)
		else:
			self.insert_agent_key(self.agent_key, module_conf)

	def generate_key(self, module_conf):
		m = self.sort_config(module_conf.copy())
		return "{prefix}_{exchange}_{data}_{module}_{market}_{copy}_{hash}".format(
			prefix=self.job_prefix, 
			exchange=m['exchange'],
			data=m['data'],
			module=m['module'],
			market=m['market'][:6],#first 6 ch only to cut length of key names
			copy=m['copy'],
			hash=sha256(json.dumps(m).encode('utf-8')).hexdigest()
		)

	def sort_config(self, conf):
		if 'restart_after' in conf:
			conf['restart_after'] = int(conf['restart_after'])
		if 'copy' in conf:
			conf['copy'] = int(conf['copy'])
		ordered = OrderedDict(sorted(conf.items()))
		return ordered

	def respawn_agent_key(self, agent_key, module_conf):
		now = datetime.datetime.utcnow().isoformat()
		self.set_dict_field(self.agent_key, 'started_at', now)
		self.set_dict_field(self.agent_key, 'last_update_at', now)
		self.set_dict_field(self.agent_key, 'status', RUNNING)
		self.set_dict_field(self.agent_key, 'wsqueue', json.dumps([]))
		self.set_dict_field(self.agent_key, 'config', json.dumps(module_conf))
		print('AGENT RESPAWN INITIAL SET')

	def agent_starting(self, agent_key):
		now = datetime.datetime.utcnow().isoformat()
		self.set_dict_field(agent_key, 'last_update_at', now)
		self.set_dict_field(agent_key, 'status', STARTING)

	def insert_agent_key(self, agent_key, module_conf, default_status=None):
		now = datetime.datetime.utcnow().isoformat()
		status = RUNNING
		if default_status is not None:
			status = default_status
		a_dict = {
			'exchange': module_conf['exchange'],
			'started_at': now,
			'last_update_at': now,
			'last_restart_reason': '',
			'status': status,
			'config': json.dumps(module_conf),
			'wsqueue': json.dumps([]),
			'data_per_last_min': 0,
		}
		
		self.set_dict(agent_key, a_dict)
		print('AGENT NEW INITIAL SET')

	def agent_is_pending(self, agent_key):
		if (self.r.exists(agent_key)):
			current_status = self.get_dict_field(agent_key, 'status')
			if current_status == PENDING:
				return True
		return False

	def agent_is_continuous(self, agent_key):
		if (self.r.exists(agent_key)):
			agent_config = self.get_agent_config(agent_key)
			if (agent_config['schedule'] == SCHEDULE_CONTINUOUS):
				if (agent_config['restart'] == RESTART_ALWAYS):
					return True
		return False

	def agent_restart_needed(self, agent_key):
		if (self.r.exists(agent_key)):
			current_status = self.get_dict_field(agent_key, 'status')
			agent_config = self.get_agent_config(agent_key)
			if (self.agent_is_continuous(agent_key)):
				now = datetime.datetime.now()
				last_update_at = dateutil.parser.parse(self.get_dict_field(agent_key, 'last_update_at'))
				diff_seconds = (now-last_update_at).total_seconds()

				restart_after = int(agent_config['restart_after'])
				
				#for running jobs we restart immediately
				if (diff_seconds > restart_after and current_status not in [STARTING]):
					return True
				#for starting jobs we restart after x*2 delay just in case they were stuck in that state
				if (diff_seconds > restart_after*2 and current_status == STARTING):
					return True
		return False

	def restart_agent(self, agent_key, last_restart_reason):
		if (self.r.exists(agent_key)):
			self.set_dict_field(agent_key, 'last_restart_reason', last_restart_reason)
			now = datetime.datetime.utcnow().isoformat()
			self.set_dict_field(agent_key, 'last_update_at', now)

	def upd_msg_uptime(self):
		if (self.r.exists(self.agent_key)):
			now = datetime.datetime.utcnow().timestamp()*1000000
			delta = 1*1000000 #1 sec threshold to update
			if ((now-self.last_update_at)>delta):
				last_update_at = datetime.datetime.utcnow().isoformat()
				self.last_update_at = now
				self.set_dict_field(self.agent_key, 'last_update_at', last_update_at)
				#print('MESSAGE UPTIME SET: {key}'.format(key=self.agent_key))
		else:
			print('MESSAGE UPTIME SKIPPED: {key} DOESNT EXIST'.format(key=self.agent_key))

	def upd_data_metrics(self, metrics):
		if (self.r.exists(self.agent_key)):
			self.set_dict_field(self.agent_key, 'data_per_last_min', metrics)
			#print('MESSAGE METRICS SET: {key}'.format(key=self.agent_key))
		else:
			print('MESSAGE METRICS SKIPPED: {key} DOESNT EXIST'.format(key=self.agent_key))

	def push_msg_pool(self, agent_key, msg):
		if (self.r.exists(agent_key)):
			wsqueue = self.get_dict_field(agent_key, 'wsqueue')
			if wsqueue is not None:
				try:
					wsqueue = json.loads(wsqueue)
				except (ValueError, KeyError) as e:
					wsqueue = []
					pass
			else:
				wsqueue = []

			wsqueue.append(msg.decode('utf-8'))
			wsqueue = json.dumps(wsqueue)
			self.set_dict_field(agent_key, 'wsqueue', wsqueue)
			#print('MESSAGE POOL PUSH SET: {key}'.format(key=agent_key))
		else:
			print('MESSAGE POOL PUSH SKIPPED: {key} DOESNT EXIST'.format(key=agent_key))

	def get_msg_pool(self):
		#only able to get messages for current assign agent
		if not self.agent_key:
			return None
		wsqueue = self.get_dict_field(self.agent_key, 'wsqueue')
		if wsqueue is not None:
			try:
				wsqueue = json.loads(wsqueue)
			except ValueError:
				wsqueue = []
				pass
			msg = next(iter(wsqueue), None)
			if msg is not None:
				msg = msg.encode('utf-8')
				self.pop_msg_pool()
			return msg
		else:
			return None

	def pop_msg_pool(self):
		wsqueue = self.get_dict_field(self.agent_key, 'wsqueue')
		if wsqueue is not None:
			try:
				wsqueue = json.loads(wsqueue)
			except ValueError:
				wsqueue = []
				pass
			wsqueue = wsqueue[1:]
			wsqueue = json.dumps(wsqueue)
			self.set_dict_field(self.agent_key, 'wsqueue', wsqueue)
			#print('MESSAGE POOL POP DONE: {key}'.format(key=self.agent_key))
		else:
			print('MESSAGE POOL POP SKIPPED: {key} POOL DOESNT EXIST'.format(key=self.agent_key))

	def sscan_keys(self, pattern):
		results = {}
		for key in self.r.scan_iter(match=pattern):
			item = self.get_dict(key)
			results[key] = item
		return results

	def get_state(self, pattern):
		return self.sscan_keys(pattern)

	#tracking every time we scraped some data
	def data_scraped(self, i):
		now = datetime.datetime.utcnow().timestamp()*1000000
		self.data_per_last_min[now] = i
		self.data_accumulate_counter += i

	def data_scrape_counter_reset(self):
		self.data_accumulate_counter = 0

	def recalc_data_metrics(self):
		now = datetime.datetime.utcnow().timestamp()*1000000
		delta = 60*1000000
		delta_items = 0
		data_per_last_min = dict(self.data_per_last_min)
		for ts in data_per_last_min.keys():
			if (now-ts)>delta:
				self.data_per_last_min.pop(ts)
			else:
				delta_items += self.data_per_last_min[ts]
		self.upd_data_metrics(delta_items)

	#getting amount of data per last X seconds
	def get_total_recent_data(self, sec):
		now = datetime.datetime.utcnow().timestamp()*1000000
		delta = sec*1000000
		delta_items = 0
		data_per_last_min = dict(self.data_per_last_min)
		for ts in data_per_last_min.keys():
			if (now-ts)<delta:
				delta_items += self.data_per_last_min[ts]
		return delta_items

	#appending pushgateway job keys with creation date
	def append_pg_key(self, job_key):
		now = datetime.datetime.utcnow().timestamp()
		self.set_dict_field(self.pg_keys_list, job_key, now)

	def make_proxy_key(self, website):
		return "PROXY_{website}".format(website=website)

	#appending good proxies to the list
	def append_proxy_key(self, website, proxy, weight):
		proxy = json.dumps(proxy)
		self.set_dict_field(self.make_proxy_key(website), proxy, weight)

	#appending good proxies to the list
	def del_proxy_key(self, website, proxy):
		proxy = json.dumps(proxy)
		self.del_dict_key(self.make_proxy_key(website), proxy)

	#getting random proxy from a list of
	def get_random_proxy(self, website):
		#get all the proxies for the website
		keys = self.get_dict(self.make_proxy_key(website))
		proxies = []
		weights = []
		for proxy, weight in keys.items():
			proxies.append(json.loads(proxy.decode('utf-8')))
			weights.append(float(weight))
		if (len(proxies) > 0):
			rand = random.choices(proxies, weights)
			return rand[0]
		else:
			return None
