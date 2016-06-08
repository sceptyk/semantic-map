#!usr/bin/env python

from collector.twitter_collector import Twitter_Collector
from collector.twitter_stream_collector import Twitter_Stream_Collector

from threading import Thread
import time
import tweepy

twitter_stream = Twitter_Stream_Collector(
	client_key = 'd7zekGyp2jiMyd65ACGsfOECy',
	client_secret = 'xqiOuIlRFE9KWnYZJ7H85yFkzePVOJhEAT1o1dIQ18LKlLppw6',
	access_token = '737973216262639618-KPr0Fvuk6AUgsm8AjCTZbBhAe3fzLN4',
	access_secret = 'TQHGxJIQ8ze01jtsy9o65FwThvIK8JdHCsFnvWkNB5yfs')

print("Twitter data ------")

twitter_strema.run()