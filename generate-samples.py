# Generates samples for air-quality aplication and publishes them in a pub/sub topic
# (that's what sensors would do if I actualy had them).
#
# Please run " gcloud auth application-default login" first to have access to your PubSub resources
# or use a dedicated Service Account.

import time, random, sys
from datetime import datetime
from google.cloud import pubsub_v1

project_id = str(sys.argv[1])
topic_name = str(sys.argv[2])

print project_id
print topic_name

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

cities = ["WARSAW", "KRAKOW", "GDANSK", "LUBLIN"]

def callback(message_future):
  if message_future.exception(timeout=30):
    print('Publishing message on {} threw an Exception {}.'.format(
      topic_name, message_future.exception()))


while True:
  city = random.choice(cities)
  level = random.randint(0, 100)
  timestamp = int(time.time())

  print u'{:>9} | {:>5} | {}'.format(city, level,
                                     datetime.fromtimestamp(timestamp).strftime('%H:%M:%S'))

  # Message payload format: <city>,<pm25level>
  payload = u'{},{}'.format(city, level).encode('utf-8')
  message_future = publisher.publish(topic_path, data=payload, timestamp=str(timestamp * 1000))
  message_future.add_done_callback(callback)
