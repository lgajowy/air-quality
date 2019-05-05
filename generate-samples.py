# Generates samples for air-quality aplication and publishes them in a pub/sub topic
# (that's what sensors would do if I actualy had them).
#
# NOTE: The script requires to setup a service account first. Otherwise returns 403 error. See:
# https://cloud.google.com/pubsub/docs/reference/libraries#client-libraries-install-python
#
import time, random, json

from google.cloud import pubsub_v1

project_id = "chromatic-idea-229612"
topic_name = "air-quality"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

cities = ["WARSAW", "KRAKOW", "KATOWICE", "WROCLAW"]


def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())

while True:
    data = {
        "timestamp": time.time(),
        "city": random.choice(cities),
        "pm25": random.randint(0, 200)
    }
    payload = json.dumps(data).encode('utf-8')

    print (payload)

    message_future = publisher.publish(topic_path, data=payload)
    message_future.add_done_callback(callback)

    time.sleep(3)
