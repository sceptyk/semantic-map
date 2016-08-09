from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
from util.util import Util
import time
import string
import math
import datetime
import os

class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	def __init__(self):
		self.conn = Mysql_Connect().get_connection()
		self.cursor = self.conn.cursor()
		self.util = Util()
		self.get_data()

	def get_data(self):
		loc_cursor = self.conn.cursor()
		chunk_size = 500
		start_time = time.strftime("2016-06-08 00:00:00")
		while True:
			sql = """SELECT * FROM tweets where timestamp > %s LIMIT %s"""
			try:
				loc_cursor.execute(sql, (start_time, chunk_size))
				results = loc_cursor.fetchall()
				if len(results)==0:
					print "Not enough twees - sleep 1hr"
					time.sleep(3600)
					continue
				replace = self.select_old_id_twt_kword()
				for row in results:
					print "New tweet"
					twt = Tweet()
					twt.populate(row)
					try:
						self.store_data(twt, replace)
					except:
						continue
				last = self.last_tweet(results[-1]).dict()
				start_time = last['time']
			except Exception, e:
				print e

	def last_tweet(self, list):
		twt = Tweet()
		twt.populate(list)
		return twt

#START: word/char elimination
	def elim_useless(self, txt):
		clear_txt = self.clear_punctuation(txt)
		list = clear_txt.split(' ')
		r_list = []
		stop = self.stopwords()
		for word in list:
			if word.lower() in stop or len(word) <= 2 or self.has_numbers(word) or self.has_http(word):
				continue
			else:
				r_list.append(word)
		return r_list

	def clear_punctuation(self, text):
		clear_string = ""
		delimiters = string.punctuation + "\n\t"
		for symbol in text:
			if symbol not in delimiters:
				clear_string += symbol
		return clear_string

	def stopwords(self):
		with open(os.path.normpath("stopwords.txt")) as input:
			text = input.readline()
		list = text.split(",")
		clear_list = []
		for word in list:
			clear_list.append(word.replace(" ", ""))
		return clear_list

	def has_numbers(self, word):
		return any(ch.isdigit() for ch in word)

	def has_http(self, word):
		return word.startswith('http')
	#END: word/char elimination

	#START: Keywords table
	def fetch_keyword_id(self, word):
		loc_cursor = self.conn.cursor()
		query = """select _id from keywords where word = %s"""
		loc_cursor.execute(query,word.lower())
		return loc_cursor.fetchall()[0][0]

	def insert_keyword(self, list):
		loc_cursor = self.conn.cursor()
		query = """INSERT IGNORE INTO keywords (word) VALUES (%s)"""
		keywords = []
		for i in range(len(list)):
			keywords.append(list[i].lower())
		try:
			loc_cursor.executemany(query, keywords)
			self.conn.commit()
		except:
			self.conn.rollback()

	#END: Keywords table

	#START: Counter table
	def insert_counter(self, kword, cloud,layer, day_time, day):
		loc_cursor = self.conn.cursor()
		query = """insert ignore into word_counter (_keyword, _cloud, _layer, day_time, day) values ('%s', %s, '%s', '%s', %s)"""
		if self.fetch_counter_id(kword, cloud, layer, day_time, day) is None:
			loc_cursor.execute(query, (kword, cloud, layer, day_time, day))
			self.conn.commit()
			self.incr_counter_count(self.fetch_counter_id(kword, cloud, layer, day_time, day))
		else:
			self.incr_counter_count(self.fetch_counter_id(kword, cloud, layer, day_time, day))

	def incr_counter_count(self, tkw_id):
		loc_cursor = self.conn.cursor()
		fetch = """update word_counter set count = count + 1 where _id = '%s'"""
		try:
			loc_cursor.execute(fetch, tkw_id)
			self.conn.commit()
		except:
			self.conn.rollback()
			raise Exception("Counter id doesnt exist")

	def fetch_counter_id(self, word, cloud, layer, day_time, day):
		loc_cursor = self.conn.cursor()
		query = """select _id from word_counter where _keyword = '%s' and _cloud = %s and _layer = '%s' and day_time = '%s' and day = %s"""
		try:
			loc_cursor.execute(query, (word, cloud, layer, day_time, day))
			return loc_cursor.fetchall()[0]
		except :
			return None

	def fetch_counter_cloud(self, id):
		loc_cursor = self.conn.cursor()
		query = """select _cloud from word_counter where _id = '%s'"""
		try:
			loc_cursor.execute(query, id)
			return loc_cursor.fetchall()[0][0]
		except:
			raise Exception("Counter doesnt exist")

	def fetch_counter_kword(self, id):
		loc_cursor = self.conn.cursor()
		query = """select _kword from word_counter where _id = '%s'"""
		try:
			loc_cursor.execute(query, id)
			return loc_cursor.fetchall()[0][0]
		except:
			raise Exception("Counter doesnt exist")
	#END: Counter table

	# START: Tweet_keyword table

	def insert_twt_kword(self, ntwt, nkwrd):
		loc_cursor = self.conn.cursor()
		query = """insert into tweet_keywords (_tweet, _keyword) values ('%s', '%s')"""
		loc_cursor.execute(query, (ntwt, self.fetch_keyword_id(nkwrd)))
		self.conn.commit()

	def select_old_tweet(self):
		loc_cursor = self.conn.cursor()
		query = """select _id from tweets where timestamp <= DATE_SUB(NOW(), INTERVAL 2 WEEK)"""
		loc_cursor.execute(query)
		try:
			return loc_cursor.fetchall()
		except: return 0

	def update_twt_kword(self, ntwt, nkwrd, replace_id):
		loc_cursor = self.conn.cursor()
		query = """update tweet_keywords set _tweet = '%s', _keyword = '%s' where _id = '%s'"""
		loc_cursor.execute(query, (ntwt, self.fetch_keyword_id(nkwrd), replace_id))
		self.conn.commit()

	def select_old_id_twt_kword(self):
		loc_cursor = self.conn.cursor()
		query = """
				SELECT tk._id FROM tweet_keywords tk
				INNER JOIN tweets t ON t._id = tk._tweet
				WHERE t.timestamp < DATE_SUB(NOW(), INTERVAL 2 WEEK)
				ORDER BY t.timestamp DESC
				LIMIT 1"""
		loc_cursor.execute(query)
		try:
			return loc_cursor.fetchall()[0]
		except:
			return 0

	def fetch_twt_kword__tweet(self, kword):
		loc_cursor = self.conn.cursor()
		query = """select _tweet from tweet_keyword where _keyword = '%s'"""
		loc_cursor.execute(query, self.fetch_keyword_id(kword))
		return loc_cursor.fetchall()[0]

	def fetch_twt_kword_kword(self, tweet_id):
		loc_cursor = self.conn.cursor()
		query = """select _tweet from tweet_keyword where _tweet = '%s'"""
		loc_cursor.execute(query, self.fetch_keyword_id(tweet_id))
		return loc_cursor.fetchall()[0]

	def fetch_twt_kword_id(self, tweet_id, kword):
		loc_cursor = self.conn.cursor()
		query = """select _id from tweet_keyword where _tweet = '%s' and _keyword = '%s' """
		loc_cursor.execute(query, (tweet_id, self.fetch_keyword_id(kword)))
		return loc_cursor.fetchall()[0]

	# END: Tweet_keyword table

	def parse_timestamp(self, timestamp):  # 2016-06-07
		week_day = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
		timestamp = str(timestamp)
		wk = week_day[datetime.date(int(timestamp[0:4]), int(timestamp[5:7]), int(timestamp[8:10])).weekday() + 1]
		t = time.strftime(timestamp[11:20])
		return wk, t

	def day_time(self, t):
		if time.strftime('04:00:00') <= t <= time.strftime('11:59:59') : return 1
		if time.strftime('12:00:00') <= t <= time.strftime('16:59:59'): return 2
		if time.strftime('17:00:00') <= t <= time.strftime('21:59:59'): return 3
		if time.strftime('22:00:00') <= t <= time.strftime('03:59:59') : return 4
		else: return 0

	def layer_index(self, precision):
		if precision == 0.2:
			return 1
		elif precision == 0.6:
			return 2
		elif precision == 1.2:
			return 3
		else: return 0

	#NB!: Major back-end method - responsible for parsing a tweet and divide it into tables in a db
	def store_data(self, tweet, replace): #id, usr, text, lat, lng, timestamp
		twt = tweet.dict()
		tweet_id = twt['_id']
		text = self.elim_useless(twt['text'])
		day = self.parse_timestamp(twt['time'])
		time_i = self.day_time(day[1])
		lyrs = [0.2,0.6,1.2] #scaling: 0.2 = 0.2*1km
		cloud = [] #must be 3 hashed values

		self.insert_keyword(text)
		#Cloud calculation and insertion
		for each in lyrs:
			cloud.append(self.util.hash_geo(twt['lat'],twt['lng'],each)) #Will need it later
		#Word counter
		for each in text:
			for index in range(0,3,1):
				self.insert_counter(self.fetch_keyword_id(each), cloud[index], self.layer_index(lyrs[index]), time_i, day[0])

		for word in text:
			replace = self.select_old_id_twt_kword()
			if replace != 0:
				self.update_twt_kword(tweet_id, word, replace[0])
			else:
				self.insert_twt_kword(tweet_id, word)

test = Cloud_Parser()