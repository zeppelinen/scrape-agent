from google.cloud import logging
from fluent import sender
import os
import datetime

STDOUT = 'stdout'
STACKDRIVER = 'stackdriver'
FLUENTD = 'fluentd'

class GCLog():

	severity_levels = {
		'ALL': 0,
		'DEBUG': 1,
		'INFO': 2,
		'WARNING': 3,
		'ERROR': 4,
		'CRITICAL': 5,
	}
	logging = STDOUT

	def __init__(self, args):
		self.set_logger(args['name'])
		self.set_severity(args['write_severity'])
		#init type of logging
		self.detect_logging()        

	def detect_logging(self):
		if 'LOGGING' in os.environ:
			if os.environ['LOGGING'] == STACKDRIVER:
				self.logging = os.environ['LOGGING']
				self.logger = logging.Client().logger(self.logger_name)
			if os.environ['LOGGING'] == FLUENTD:
				self.logging = os.environ['LOGGING']
				#defaults for fluentd
				f_host = 'fluentd'
				f_port = 24224
				if 'FLUENTD_HOST' in os.environ:
					f_host = os.environ['FLUENTD_HOST']
				if 'FLUENTD_PORT' in os.environ:
					f_port = os.environ['FLUENTD_PORT']
				self.logger = sender.setup(self.logger_name, host=f_host, port=f_port)
		

	def set_logger(self, logger_name):
		env = os.environ['ENV']
		self.logger_name = logger_name+'-'+env

	def set_severity(self, severity):
		if severity not in self.severity_levels:
			severity = 'ALL'
		self.write_severity = severity

	# [START logging_write_log_entry]
	def write(self, msg, severity='ALL'):
		if severity not in self.severity_levels:
			severity = 'ALL'

		#skip writing this log if message severity is lower than class severity
		if (self.severity_levels[severity] < self.severity_levels[self.write_severity]):
			#print('Log is lower than write severity level, skipping...')
			return 0

		"""Writes log entries to the given logger."""
		if self.logging == STACKDRIVER:
			if (isinstance(msg, str)):
				self.logger.log_text(msg, severity=severity)
			elif (isinstance(msg, dict)):
				# Struct log. The struct can be any JSON-serializable dictionary.
				self.logger.log_struct(msg, severity=severity)
		if self.logging == FLUENTD:
			if (isinstance(msg, str)):
				logger.emit(severity, {'msg': msg})
			elif (isinstance(msg, dict)):
				# Struct log. The struct can be any JSON-serializable dictionary.
				logger.emit(severity, msg)
		if self.logging == STDOUT:
			print('{date}: [{severity}]: {msg}'.format(date=datetime.datetime.utcnow().isoformat(), severity=severity, msg=repr(msg)), flush=True)

		#print('Wrote logs to {}:{}.'.format(self.logging, self.logger_name))
	# [END logging_write_log_entry]


	# [START logging_list_log_entries]
	def list(self):
		"""Lists the most recent entries for a given logger."""
		print('Listing entries for logger {}:'.format(self.logger.name))

		for entry in self.logger.list_entries():
			timestamp = entry.timestamp.isoformat()
			print('* {}: {}'.format
				  (timestamp, entry.payload))
	# [END logging_list_log_entries]


	# [START logging_delete_log]
	def delete(self):
		"""Deletes a logger and all its entries.
		Note that a deletion can take several minutes to take effect.
		"""
		self.logger.delete()

		print('Deleted all logging entries for {}'.format(self.logger.name))
	# [END logging_delete_log]