# This code harvest Geotweets then MessagePacks them and populate a Kafka Topic.
# Author:Ryan Qi Wang
# Modified by: Devika Kakkar
# Modified on: October 4, 2016

import sys
import tweepy
import cPickle as pickle
import datetime
import msgpack
import json
import time
from kafka import KafkaProducer
from kafka import KafkaConsumer
import os
import lz4
import lz4tools
import xxhash


# Authorization credentials

# Authorization credentials


CONSUMER_KEY = os.environ["CONSUMER_KEY"]
CONSUMER_SECRET = os.environ["CONSUMER_SECRET"]
ACCESS_KEY = os.environ["ACCESS_KEY"]
ACCESS_SECRET = os.environ["ACCESS_SECRET"]

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

#Class CustomStreamListener

class CustomStreamListener(tweepy.StreamListener):
 
    def on_status(self, status):
        try:
            if status.coordinates!=None:
                status_json= status._json
                packed = msgpack.packb(status_json)
                producer.send(os.environ["TWEET_TOPIC"], packed)
                #unpack= msgpack.unpackb(packed)
                #print >> sys.stderr, 'ID ', (unpack["id"])
              
                

        except Exception, e:
            print >> sys.stderr, 'Encountered Exception:', e
            print datetime.datetime.now()
            pass

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        print datetime.datetime.now()
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        print datetime.datetime.now()
        return True # Don't kill the stream


#Kafka Producer
time.sleep(10)

producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_HOST_PORT"],compression_type='lz4')
#consumer = KafkaConsumer(bootstrap_servers='localhost:9092',value_deserializer=msgpack.loads)
#consumer.subscribe(['GeoTweets'])

# Create a streaming API and set a timeout value of 60 seconds

while True:
    try: 
        streaming_api1 = tweepy.streaming.Stream(auth, CustomStreamListener(), timeout=30)
        
        print >> sys.stderr, 'Filtering the public timeline for '       

        streaming_api1.filter(locations=[-180,-90,180,90])


        
    except Exception, e:
                print >> sys.stderr, 'Encountered Exception:', e
                print datetime.datetime.now()
                pass
