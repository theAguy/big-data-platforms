### Stav Vaknin: 313581654
### Guy Assa: 204118616

import pykafka
import json
import socket
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

#TWITTER API CONFIGURATIONS
consumer_key = "2PWSknG2mtZO093FUf2iA644C"
consumer_secret = "9rI5GsFTfXK2PV8gLLT197b0P9wyKfo2vqTmGN3MEixbhdsXyB"
access_token = "1399004697072128002-WuPPCzcoUpvFcm7VkN2R7UvOVCUp66"
access_secret = "aX4slC3sazRgSJvL8o7YnVRaCsiwmW0IZqB0qnzLauVwi"

#TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

#Twitter Stream Listener
class KafkaPushListener(StreamListener):
        def __init__(self):
            self.client = pykafka.KafkaClient("localhost:9092")
            self.producer = self.client.topics[bytes("twitter", "ascii")].get_producer()            
  
        def on_data(self, data):
            self.producer.produce(bytes(data, "ascii"))
            return True            
                                                                                
        def on_error(self, status): # return error message                     
            print(status)
            return True

#Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())
#Produce Data that has Covid19 hashtag (Tweets)
twitter_stream.filter(track=['#Covid19'])
