import json
from datetime import datetime
import os
import sys
import subprocess
import importlib
import time
import uuid
import dateutil.parser
import traceback
import logging as lg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'modules'))
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from gclog import *
from qmetrics import *
from qredis import *
from qmysql import *
from syshelper import *

try:
	import asyncio
except ImportError:
	import trollius as asyncio

KUBERNETES = os.environ['KUBERNETES']
if (KUBERNETES == 'true'):
	KUBERNETES = True
else:
	KUBERNETES = False

gclog = GCLog({
	"name": "historical-scheduler-log",
	"write_severity": "ALL",
})

REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
redis = QRedis({
	"host": "redis",
	"port": 6379,
	"password": REDIS_PASSWORD
})

RUNNING_JOBS_LIMIT = 3

#monitor new scheduled jobs and spawn new if there are less then X running
def run_spawn_monitor(scheduler):
	try:
		db = QMySql()

		#init metrics gathering
		metrics = QMetrics({'job_key': 'job_historical_scheduler'})
		pending_jobs_total = metrics.create_gauge('historical_scheduler_pending_jobs_total', 'PENDING Historical trades jobs', ['exchange'])
		working_jobs_total = metrics.create_gauge('historical_scheduler_working_jobs_total', 'WORKING Historical trades jobs', ['exchange'])

		while True:
			db.connect()

			#reset stuck jobs
			db.execute(
				"UPDATE `trades_period` \
				SET `trades_period`.`status` = 'PENDING', `trades_period`.`container_name` = NULL, `trades_period`.`started_at` = NULL \
				WHERE \
				`trades_period`.`status` = 'WORKING' AND \
				(UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(`trades_period`.`started_at`)) > (`trades_period`.`cutoff` + 30)"
			)
			db.commit()

			#getting number of current running/pending jobs
			working = db.fetchone("SELECT COUNT(`id`) FROM `trades_period` WHERE `status` = 'WORKING'")
			pending = db.fetchone("SELECT COUNT(`id`) FROM `trades_period` WHERE `status` = 'PENDING'")
			gclog.write('STATUS: {working} WORKING, {pending} PENDING'.format(working=working[0], pending=pending[0]), 'DEBUG')
			if (working[0] < RUNNING_JOBS_LIMIT and pending[0] > 0):
				spawn_jobs = RUNNING_JOBS_LIMIT-working[0]
				gclog.write('WE HAVE {working} WORKING JOBS'.format(working=working[0]), 'DEBUG')
				gclog.write('LET\'S SPAWN {spawn_jobs} NEW JOBS'.format(spawn_jobs=spawn_jobs), 'DEBUG')
				spawn_list = db.fetchall(
					"SELECT \
						`trades_period`.`id`,\
						`trades_schedule`.`exchange`, \
						`trades_schedule`.`module`, \
						`trades_schedule`.`market`, \
						`trades_schedule`.`url`, \
						`trades_period`.`starttime`, \
						`trades_period`.`endtime`, \
						`trades_period`.`cutoff` \
						FROM `trades_schedule`\
					LEFT JOIN `trades_period` ON `trades_period`.`schedule_id`=`trades_schedule`.`id` \
					WHERE `trades_period`.`status` = 'PENDING'\
					LIMIT %(spawn_jobs)s", {'spawn_jobs': spawn_jobs})
				for spawn in spawn_list:
					period_id = spawn[0]
					config = {
						'data': 'TRADES',
						'exchange': spawn[1],
						'module': spawn[2],
						'market': spawn[3],
						'url': spawn[4],
						'starttime': spawn[5],
						'endtime': spawn[6],
						'cut_off_window': spawn[7],
						'schedule': 'cron',
						'copy': 1
					}
					container_name = uuid.uuid4().hex
					#update period record
					#real container name to be created will be prefixed with module name
					container_name_prefixed = "{module}-{container_name}".format(module=spawn[2], container_name=container_name)
					db.execute(
						"UPDATE `trades_period` SET \
							`status` = 'WORKING',\
							`started_at` = NOW(), \
							`container_name` = %(container_name)s\
						WHERE `trades_period`.`id` = %(period_id)s", 
						{'container_name': container_name_prefixed, 'period_id': period_id}
					)
					db.commit()
					
					agent_key = redis.generate_key(config)
					redis.insert_agent_key(agent_key, config, 'PENDING')
					
					scheduler.add_job(spawn_agent, None, [agent_key, config, container_name])

			#getting metrics from DB
			working_grouped = db.fetchall(
				"SELECT \
				SUM(CASE WHEN `trades_period`.`status` = 'WORKING' THEN 1 ELSE 0 END) AS `sum`, \
				`trades_schedule`.`exchange` \
				FROM `trades_schedule` \
				LEFT JOIN `trades_period` ON `trades_period`.`schedule_id`=`trades_schedule`.`id` \
				GROUP BY `trades_schedule`.`exchange`")
			pending_grouped = db.fetchall(
				"SELECT \
				SUM(CASE WHEN `trades_period`.`status` = 'PENDING' THEN 1 ELSE 0 END) AS `sum`, \
				`trades_schedule`.`exchange` \
				FROM `trades_schedule` \
				LEFT JOIN `trades_period` ON `trades_period`.`schedule_id`=`trades_schedule`.`id` \
				GROUP BY `trades_schedule`.`exchange`")
			for row in working_grouped:
				#print("SETTING {num} WORKING jobs for {exchange}".format(num=row[0], exchange=row[1]))
				metrics.gauge_set(working_jobs_total, *[row[1]], value=row[0])
			for row in pending_grouped:
				#print("SETTING {num} PENDING jobs for {exchange}".format(num=row[0], exchange=row[1]))
				metrics.gauge_set(pending_jobs_total, *[row[1]], value=row[0])
			job_key = metrics.push_metrics()
			#gclog.write('PUSHED SOME METRICS', 'DEBUG')
			redis.append_pg_key(job_key)

			db.commit()
			db.close()
			time.sleep(5)

	except Exception as ex:
		gclog.write({
			'message': "[HISTORICAL SCHEDULER DEAD]: Scheduler exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
			'error': traceback.format_exc(),
		}, 'ERROR')
		os._exit(1)

def get_cmd_cron(agent_key, config, container_name):
	if (KUBERNETES):
		script = "start-job-kubernetes.sh"
	else:
		script = "start-container.sh"
	cmd = 'scheduler/{script} {modulepy} {agent_key} {container_name}'.format(
		script=script,
		modulepy=config['module'].lower(),
		agent_key=agent_key,
		container_name=container_name
	)
	return cmd

def spawn_agent(agent_key, config, container_name):
	cmd = get_cmd_cron(agent_key, config, container_name)

	gclog.write({
		"message": "Spawning new agent",
		"config": config,
		"cmd": cmd,
	}, 'DEBUG')

	run_cmd(cmd, shell=True)

def start_agent(config):
	#new container name
	config['container_name'] = uuid.uuid4().hex #unique container name
	#gclog.write('Spawn container on cron trigger for agent: {agent_key}'.format(agent_key=agent_key), 'DEBUG')
	spawn_agent(config)
	return 

if __name__ == '__main__':

	def main():

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

		#lg.basicConfig()
		#lg.getLogger('apscheduler').setLevel(lg.DEBUG)

		#running monitor first
		scheduler.add_job(run_spawn_monitor, None, [scheduler])

		scheduler.start()
		print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

		gclog.write('[HISTORICAL SCHEDULER ALIVE]', 'DEBUG')

		# Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
		try:
			asyncio.get_event_loop().run_forever()
		except (KeyboardInterrupt, SystemExit):
			pass

	try:
		main()
	except Exception as ex:
		gclog.write({
			'message': "[HISTORICAL SCHEDULER DEAD]: Scheduler exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
			'error': traceback.format_exc(),
		}, 'ERROR')
		os._exit(1)