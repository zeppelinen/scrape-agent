#!/usr/bin/env python

import argparse
import code
import sys
import threading
import time
import ssl
import datetime

import six
from six.moves.urllib.parse import urlparse

import websocket

try:
	import readline
except ImportError:
	pass


def get_encoding():
	encoding = getattr(sys.stdin, "encoding", "")
	if not encoding:
		return "utf-8"
	else:
		return encoding.lower()


OPCODE_DATA = (websocket.ABNF.OPCODE_TEXT, websocket.ABNF.OPCODE_BINARY)
ENCODING = get_encoding()


class VAction(argparse.Action):

	def __call__(self, parser, args, values, option_string=None):
		if values is None:
			values = "1"
		try:
			values = int(values)
		except ValueError:
			values = values.count("v") + 1
		setattr(args, self.dest, values)


class RawInput:

	def raw_input(self, prompt):
		if six.PY3:
			line = input(prompt)
		else:
			line = raw_input(prompt)

		if ENCODING and ENCODING != "utf-8" and not isinstance(line, six.text_type):
			line = line.decode(ENCODING).encode("utf-8")
		elif isinstance(line, six.text_type):
			line = line.encode("utf-8")

		return line


class InteractiveConsole(RawInput, code.InteractiveConsole):

	def write(self, data):
		sys.stdout.write("\033[2K\033[E")
		# sys.stdout.write("\n")
		sys.stdout.write("\033[34m< " + data + "\033[39m")
		sys.stdout.write("\n> ")
		sys.stdout.flush()

	def read(self):
		return self.raw_input("> ")


class NonInteractive(RawInput):

	def write(self, data):
		sys.stdout.write(data)
		sys.stdout.write("\n")
		sys.stdout.flush()

	def read(self):
		return self.raw_input("")


class Qsocket():

	def __init__(self, args={}):

		self.args = args

		#default params if they were no initiated
		if "verbose" not in args:
			args["verbose"] = 0
		if "proxy" not in args:
			args["proxy"] = False
		if "origin" not in args:
			args["origin"] = False
		if "subprotocols" not in args:
			args["subprotocols"] = False
		if "nocert" not in args:
			args["nocert"] = False
		if "headers" not in args:
			args["headers"] = False
		if "raw" not in args:
			args["raw"] = False
		if "text" not in args:
			args["text"] = False
		if "timings" not in args:
			args["timings"] = 0

		self.start_time = time.time()
		if self.args['verbose'] > 1:
			websocket.enableTrace(True)
		options = {}
		if self.args['proxy']:
			p = urlparse(self.args['proxy'])
			options["http_proxy_host"] = p.hostname
			options["http_proxy_port"] = p.port
		if self.args['origin']:
			options["origin"] = self.args['origin']
		if self.args['subprotocols']:
			options["subprotocols"] = self.args['subprotocols']
		opts = {}
		if self.args['nocert']:
			opts = {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}
		if self.args['headers']:
			options['header'] = map(str.strip, self.args['headers'].split(','))

		self.ws = websocket.create_connection(self.args['url'], sslopt=opts, **options)	

		if self.args['raw']:
			self.console = NonInteractive()
		else:
			self.console = InteractiveConsole()
			print("Press Ctrl+C to quit")

		thread = threading.Thread(target=self.recv_ws)
		thread.daemon = True
		thread.start()

		if self.args['text']:
			self.msg_send(self.args['text'])

		return None

	def recv(self):
		try:
			frame = self.ws.recv_frame()
		except websocket.WebSocketException:
			return websocket.ABNF.OPCODE_CLOSE, None
		if not frame:
			raise websocket.WebSocketException("Not a valid frame %s" % frame)
		elif frame.opcode in OPCODE_DATA:
			return frame.opcode, frame.data
		elif frame.opcode == websocket.ABNF.OPCODE_CLOSE:
			self.ws.send_close()
			return frame.opcode, None
		elif frame.opcode == websocket.ABNF.OPCODE_PING:
			self.ws.pong(frame.data)
			return frame.opcode, frame.data

		return frame.opcode, frame.data

	def recv_ws(self):
		while True:
			opcode, data = self.recv()
			msg = None
			if six.PY3 and opcode == websocket.ABNF.OPCODE_TEXT and isinstance(data, bytes):
				data = str(data, "utf-8")
			if not self.args['verbose'] and opcode in OPCODE_DATA:
				msg = data
			elif self.args['verbose']:
				msg = "%s: %s" % (websocket.ABNF.OPCODE_MAP.get(opcode), data)

			if msg is not None:
				self.msg_received(msg)

			if opcode == websocket.ABNF.OPCODE_CLOSE:
				break

	def msg_received(self, msg):
		#self.console.write(msg)
		return

	def msg_send(self, msg):
		self.ws.send(msg)
		#print('MESSAGE SENT: '+msg.decode('utf-8'))
		return