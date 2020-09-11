import argparse
import os
import sys
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from gclog import *
from qmysql import *

gclog = GCLog({
	"name": "historical-scheduler-log",
	"write_severity": "ALL",
})

def main(args):
	try:
		db = QMySql()
		db.connect()
		sql_insert_query = """ INSERT INTO `trades_schedule`(`exchange`, `module`, `market`, `url`, `starttime`, `endtime`, `step`) 
			VALUES (%(exchange)s, %(module)s, %(market)s, %(url)s, %(starttime)s, %(endtime)s, %(step)s)"""
		insert_values = {
			'exchange': args['exchange'],
			'module': 	args['module'],
			'market': 	args['market'],
			'url': 		args['url'],
			'starttime': args['starttime'],
			'endtime': 	args['endtime'],
			'step': 	args['step'],
		}
		db.execute(sql_insert_query, insert_values)
		db.commit()

		schedule_id = db.lastid()

		step = int(args['step'])
		cutoff = int(args['cutoff'])

		#make time periods lower than step
		time_ranges = []
		args['starttime'] = int(args['starttime'])
		args['endtime'] = int(args['endtime'])
		if (args['endtime'] - args['starttime']) < step:
			time_ranges.append([args['starttime'], args['endtime']])
		else:
			start = args['starttime']
			end = args['endtime']
			while start + step < end:
				time_ranges.append([start, start + step])
				start += (step + 1)
			time_ranges.append([start, end])

		#inserting time periods
		time_period_insert_query = """ INSERT INTO `trades_period`(`schedule_id`, `starttime`, `endtime`, `cutoff`) 
			VALUES (%(schedule_id)s, %(starttime)s, %(endtime)s, %(cutoff)s)"""
		for ranges in time_ranges:
			insert_values = {
				'schedule_id'	: schedule_id,
				'starttime'		: ranges[0],
				'endtime'		: ranges[1],
				'cutoff'		: cutoff,
			}
			db.execute(time_period_insert_query, insert_values)
		db.commit()
		db.close()

		gclog.write("Record inserted successfully", "DEBUG")

	except Exception as ex:
		gclog.write({
			'message': "[HISTORICAL SCHEDULER DEAD]: Scheduler exception of type {0} occurred. Arguments:\n{1!r}".format(type(ex).__name__, ex.args),
			'error': traceback.format_exc(),
		}, 'ERROR')
		os._exit(1)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Schedule high level tasks to be scraped')
	parser.add_argument('exchange', help='Exchange name, string, required')
	parser.add_argument('module', help='Python agent module, string, required')
	parser.add_argument('market', help='Target MARKET (e.g. BNBBTC), string, required')
	parser.add_argument('url', help='Target URL (e.g. https://api.binance.com/api/v1/depth?symbol=[MARKET]&limit=1000), string, required')
	parser.add_argument('starttime', help='start time setting, UTC timestamp in ms, required')
	parser.add_argument('endtime', help='end time setting, UTC timestamp in ms, required')
	parser.add_argument('step', help='# of Agent copy running, int, required')
	parser.add_argument('cutoff', help='cut off, seconds, int, required')
	args = parser.parse_args()
	main({
		"exchange": args.exchange,
		"module": args.module,
		"market": args.market,
		"url": args.url,
		"starttime": args.starttime,
		"endtime": args.endtime,
		"step": args.step,
		"cutoff": args.cutoff
	})