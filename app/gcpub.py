from google.cloud import pubsub_v1
import os
import json
from queue import *
from threading import Thread
import time

class GCPub():

	num_fetch_threads = 10
	max_batch_size = 100
	max_waiting_time = 0.5 #each batch will be sent after max_waiting_time regardless how small it is

	total_queued = 0
	total_published = 0

	def __init__(self, args={}):
		if 'num_fetch_threads' in args:
			self.num_fetch_threads = int(args['num_fetch_threads'])
		if 'max_batch_size' in args:
			self.max_batch_size = int(args['max_batch_size'])
		if 'simulate' in args:
			self.simulate = args['simulate']
		else:
			self.simulate = False
		self.topic_name = args['topic_name']
		self.project_id = os.environ['GOOGLE_PUBSUB_PROJECT_ID']
		self.publisher = pubsub_v1.PublisherClient()
		self.topic_path = self.get_topic_path()

		self.queue = Queue()
		for i in range(self.num_fetch_threads):
			worker = Thread(target=self.consume, args=(i,))
			worker.setDaemon(True)
			worker.start()
		#self.queue.join()

	def write(self, message):
		#assume the message is json
		message = json.dumps(message)
		#print('QUEUEING ITEM {message}'.format(message=message))
		self.total_queued += 1
		self.queue.put(message)

	def get_topic_path(self):
		# The `topic_path` method creates a fully qualified identifier
		# in the form `projects/{project_id}/topics/{topic_name}`
		return self.publisher.topic_path(self.project_id, self.topic_name)

	def is_queue_empty(self):
		return self.queue.empty()

	def consume(self, i):
		while True:
			#print('{i}: Looking for the next msg'.format(i=i))

			time.sleep(self.max_waiting_time)

			first_item = self.queue.get()
			batch = [json.loads(first_item)]  # block until at least 1
			try:  # add more until `q` is empty or `n` items obtained
				while len(batch) < self.max_batch_size:
					item = self.queue.get(block=False)
					batch.append(json.loads(item))
			except Empty:
				pass
			
			if (len(batch)>0):
			
				batch_str = json.dumps(batch).encode('utf-8')
				#print('{i}: SENDING: {batch}'.format(i=i, batch=batch))

				# When you publish a message, the client returns a future.
				if self.simulate is False:
					future = self.publisher.publish(self.topic_path, data=batch_str)
				#print('{i}: Published {batch} of message ID {future}.'.format(i=i, batch=batch, future=future.result()))
				self.total_published += len(batch)
				self.queue.task_done()