from google.cloud import pubsub_v1
import datetime
import os


project_id = os.environ['GOOGLE_PUBSUB_PROJECT_ID']
topic_name = "dev-trades"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_name}`
topic_path = publisher.topic_path(project_id, topic_name)

for n in range(1, 10):
	last_update = datetime.datetime.utcnow().isoformat()
	message = '{"exchange-id": "BINANCE","asset_id": "BTCEURT","id": "EXCHANGE_MESSAGE_ID","timestamp": "'+last_update+'","price": 770.000000000,"volume": 3252,}'
	# Data must be a bytestring
	message = message.encode('utf-8')
	# When you publish a message, the client returns a future.
	future = publisher.publish(topic_path, data=message)
	print('Published {} of message ID {}.'.format(message, future.result()))

print('Published messages.')