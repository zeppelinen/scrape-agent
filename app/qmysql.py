import os
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode

class QMySql():

	#def __init__(self):
		#self.connect()

	def connect(self):
		self.connection = mysql.connector.connect(host=os.environ['SA_HOST'],
											  database=os.environ['SA_DATABASE'],
											  user=os.environ['SA_USER'],
											  password=os.environ['SA_PASSWORD'])
		self.cursor = self.connection.cursor()

	def _execute(self, sql, args={}):
		return self.cursor.execute(sql, args)

	def commit(self):
		self.connection.commit()

	def execute(self, sql, args={}):
		return self._execute(sql, args)

	def fetchone(self, sql, args={}):
		self._execute(sql, args)
		return self.cursor.fetchone()

	def fetchall(self, sql, args={}):
		self._execute(sql, args)
		return self.cursor.fetchall()

	def fetchmany(self, sql, args={}):
		self._execute(sql, args)
		return self.cursor.fetchmany()

	def lastid(self):
		return self.cursor.lastrowid

	def close(self):
		self.cursor.close()
		self.connection.close()
	