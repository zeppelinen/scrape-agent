import redis
import threading
import sys
import os
sys.path.append(os.path.join(os.path.dirname(sys.path[0]),'app'))
from qsocket import *


#os.environ['REDIS_PASSWORD']
redis_pass = "redis_pass"

try:
	r = redis.Redis(host='redis', port=6379, db=0, password=redis_pass)
except:
	print('couldnt connect to Redis')

script_name = os.path.basename(__file__)

def msg_received_callback(msg):
	print('CATCHING: ' + msg)

ws = Qsocket({
	"script_name": script_name,

	"url": "wss://stream.binance.com:9443/ws/bnbbtc@depth",
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

	"redis": r,
})