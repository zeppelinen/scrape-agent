import json
from datetime import datetime
import os
import sys
import subprocess
import importlib
import time
import uuid
import dateutil.parser
from apscheduler.schedulers.asyncio import AsyncIOScheduler

sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'modules'))
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from qredis import *
from gclog import *

try:
	import asyncio
except ImportError:
	import trollius as asyncio

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
gclog = GCLog({
	"name": "scheduler-log",
	"write_severity": "ALL",
})

def str_to_class(module_name, class_name):
	module = importlib.import_module(module_name)
	class_ = getattr(module, class_name)
	return class_

def stop_container(name):
	gclog.write('Stopped hung agent: '+name, 'DEBUG')
	return subprocess.run('docker stop $(docker ps -a -q --filter name={name} --format="{{{{.ID}}}}")'.format(name=name), shell=True)

def run_monitor(scheduler):
	redis = QRedis({
		"host": "redis",
		"port": 6379,
		"password": REDIS_PASSWORD
	})
	while True:
		time.sleep(1)
		processes = redis.get_state('QSOCKET*')
		now = datetime.datetime.now()
		for script, state in processes.items():
			last_update = dateutil.parser.parse(state[b'last_update'])
			diff_seconds = (now-last_update).total_seconds()

			print(script)
			print(state)
			#initial_config = json.loads(state[b'config'])
			#restart_after = int(initial_config['restart_after'])
			#print("Last message for {script} seen {diff_seconds} seconds ago".format(script=script, diff_seconds=diff_seconds))
			#if (diff_seconds > restart_after):
			#	redis.r.delete(script)
			#	print("Dropped {script} key".format(script=script))

			#	print('STOPPING CONTAINER '+state[b'container_uid'].decode('utf-8'))
			#	scheduler.add_job(stop_container, None, [state[b'container_uid'].decode('utf-8')])

				#set new UID for container
			#	initial_config['container_uid'] = uuid.uuid4().hex
			#	scheduler.add_job(start_container, None, [initial_config])

def start_container(config):
	gclog.write('Spawned new agent', 'DEBUG')
	gclog.write(config, 'DEBUG')
	subprocess.run([
		'scheduler/start-container.sh', 
		config['module'].lower(),
		str(config['exchange']),
		str(config['url']),
		str(config['market']),
		str(config['module']),
		str(config['schedule']),
		str(config['restart']),
		str(config['restart_after']),
		str(config['container_uid'])
	])


if __name__ == '__main__':

	scheduler = AsyncIOScheduler()
	scheduler.configure({
		'apscheduler.executors.default': {
			'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
			'max_workers': '1000'
		},
		'apscheduler.executors.processpool': {
			'type': 'processpool',
			'max_workers': '1000'
		},
		'apscheduler.timezone': 'UTC',
	})

	#running monitor first
	scheduler.add_job(run_monitor, None, [scheduler])
	
	scheduler.start()
	print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

	# Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
	try:
		asyncio.get_event_loop().run_forever()
	except (KeyboardInterrupt, SystemExit):
		pass