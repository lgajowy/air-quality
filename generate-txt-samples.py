# Generates samples for air-quality aplication and publishes them in a pub/sub topic
# (that's what sensors would do if I actualy had them).
#
# Please run " gcloud auth application-default login" first to have access to your PubSub resources
# or use a dedicated Service Account.

import time, random

from google.cloud import pubsub_v1


cities = ["WARSAW", "KRAKOW", "LUBLIN", "WROCLAW", "KATOWICE"]

i = 0
while i < 100000:
    # Message payload format: <city>,<pm25level>,<timestamp>
    timestamp = str(int(time.time() * 1000))
    data = u'{},{},{}'.format(random.choice(cities), random.randint(0, 200), timestamp).encode('utf-8')
    print data
    i += 1
