import threading
import sys
import os
sys.path.append("/root/ws/app")
from qsocket import *
import redis

def listen_thread(script_name, redis):
	def msg_received_callback(msg):
		print('CATCHING: ' + msg)

	ws = Qsocket({
		"script_name": script_name,

		"url": "wss://www.bitmex.com/realtime",
		"verbose": 0,
		"proxy": False,
		"origin": False,
		"subprotocols": False,
		"nocert": False,
		"headers": False,
		"raw": False,
		"text": False,
		"timings": False,

		"msg_received_callback": msg_received_callback,

		"redis": redis,
	})

def command_sequence(script_name, command, redis):
	command = command.encode('utf-8')
	#retrieving existing data
	s_dict = redis.hgetall(script_name)
	if s_dict is not None:
		s_dict['msg'] = command
	else:
		s_dict = {}
		s_dict['msg'] = command

	#set new message for script queue
	redis.hmset(script_name, s_dict)


r = redis.Redis(host='localhost', port=6379, db=0, password="1lGIV1pgLMS0HaC5Pu8IuqmoukhvYIoAkRE")

script_name = os.path.basename(__file__)

listener = threading.Thread(target=listen_thread, args=(script_name, r)) 
redis_s = threading.Thread(target=command_sequence, args=(script_name, '{"op": "subscribe", "args": ["trade"]}', r)) 

listener.start() 
redis_s.start() 

listener.join() 
redis_s.join() 