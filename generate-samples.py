# Generates samples for air-quality aplication and publishes them in a pub/sub topic
# (that's what sensors would do if I actualy had them).
#
# Please run " gcloud auth application-default login" first to have access to your PubSub resources
# or use a dedicated Service Account.

import time, random

from google.cloud import pubsub_v1

project_id = "chromatic-idea-229612"
topic_name = "air-quality"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

cities = ["WARSAW"]


def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())

while True:
    # Message payload format: <city>,<pm25level>
    data = u'{},{}'.format(random.choice(cities), random.randint(0, 200)).encode('utf-8')
    timestamp = str(int(time.time() * 1000))

    print u'Sensor data: {} Timestamp: {}'.format(data, timestamp)

    message_future = publisher.publish(topic_path, data=data, timestamp=timestamp)
    message_future.add_done_callback(callback)

    time.sleep(3)
