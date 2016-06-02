#!usr/bin/python

from collector.foursquare_collector import Foursquare_Collector
from collector.osm_collector import Osm_Collector
from collector.twitter_collector import Twitter_Collector

twitter = Twitter_Collector(
	client_key = 'd7zekGyp2jiMyd65ACGsfOECy',
	client_secret = 'xqiOuIlRFE9KWnYZJ7H85yFkzePVOJhEAT1o1dIQ18LKlLppw6',
	access_token = '737973216262639618-KPr0Fvuk6AUgsm8AjCTZbBhAe3fzLN4',
	access_secret = 'TQHGxJIQ8ze01jtsy9o65FwThvIK8JdHCsFnvWkNB5yfs')

print("Twitter data ------")
twitter.store_data()