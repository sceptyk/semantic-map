from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
import time
import string
import math
import datetime
import os


class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	size_w, size_h = 64, 64
	EDGE = 0.00000000001
	def __init__(self):
		self.conn = Mysql_Connect().get_connection()
		self.cursor = self.conn.cursor()

	def process(self, data):
		emoticons_str = r"""
		    (?:
		        [:=;] # Eyes
		        [oO\-]? # Nose (optional)
		        [D\)\]\(\]/\\OpP] # Mouth
		    )"""
		 
		regex_str = [
		    emoticons_str,
		    r'<[^>]+>', # HTML tags
		    r'(?:@[\w_]+)', # @-mentions
		    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
		    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
		 
		    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
		    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
		    r'(?:[\w_]+)', # other words
		    r'(?:\S)' # anything else
		]
		    
		tokens_re = regex_str.compile(r'('+'|'.join(regex_str)+')', regex_str.VERBOSE | regex_str.IGNORECASE)
		emoticon_re = emoticons_str.compile(r'^'+emoticons_str+'$', emoticons_str.VERBOSE | emoticons_str.IGNORECASE)
		 
		def tokenize(s):
			return tokens_re.findall(s)
		 
		def preprocess(s, lowercase=False):
			tokens = tokenize(s)
			if lowercase:
				tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
			return tokens
		return preprocess(data)

	def get_data(self):
		#TODO connect to db
		loc_cursor = self.conn.cursor()
		chunk_size = 10
		start = 0
		end = chunk_size
		while True:
			sql = """SELECT * FROM tweets ORDER BY _id LIMIT %s, %s"""

			try:
				loc_cursor.execute(sql, (start, end))
				results = loc_cursor.fetchall()
				for row in results:
					twt = Tweet()
					twt.populate(row)
					self.store_data(twt)
					if self.store_data(twt)==0:
						continue
				start = end
				end += chunk_size
			except Exception, e:
				tweet = twt.dict()
				print tweet['_id']
				print str(e)

	def get_grid(self):
		loc_rix = []
		start_lat = 53.39806981341725
		for i in range(self.size_h):
			loc_rix.append([])
			start_lng = -6.3686370849609375
			for j in range(self.size_w):
				loc_rix[i].append((start_lat, start_lng))
				start_lng += 0.00488173872
			start_lat -= 0.003145015
		return loc_rix

	def test_grid(self):
		for i in range(self.size_h):
			for j in range(self.size_w):
				print(self.Matrix[i][j])
		print("Test separate coords")
		print(self.Matrix[0][0])
		print(self.Matrix[0][0][1])

	def reset_increment(self):
		clear = """truncate table cloud"""
		self.cursor.execute(clear)

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
		with open(os.path.normpath("processor/stopwords.txt")) as input:
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

	#START: Helper functions to calculate metres per 1 degree considering the Earth's elevation
	def rlat(self, deg):
		return (deg * math.pi) / 180

	def metres_per_lat(self, rlat):
		return 111132.92 - 559.82 * math.cos(2 * rlat)

	def rlng(self, deg):
		return (deg * math.pi) / 180

	def metres_per_lng(self, rlng):
		return 111412.84 * math.cos(rlng) - 93.5 * math.cos(3 * rlng)
	#END: Helper functions to calculate metres per 1 degree considering the Earth's elevation

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
			if self.kword_exist(list[i]) != 1:
				keywords.append(list[i].lower())
			else:
				continue
		try:
			loc_cursor.executemany(query, keywords)
			self.conn.commit()
		except:
			self.conn.rollback()

	def kword_exist(self, word):
		loc_cursor = self.conn.cursor()
		query = """select 1 from keywords where word = %s"""
		loc_cursor.execute(query, word)
		try:
			return loc_cursor.fetchall()[0][0]
		except:
			return 0
	#END: Keywords table

	#START: Counter table
	def insert_counter(self, kword, cloud):
		loc_cursor = self.conn.cursor()
		query = """insert into word_counter (_keyword, _cloud) values ('%s', '%s')"""
		if self.fetch_counter_id(kword, cloud) is None:
			loc_cursor.execute(query, (kword, cloud))
			self.conn.commit()
			self.incr_counter_count(self.fetch_counter_id(kword, cloud))
		else:
			self.incr_counter_count(self.fetch_counter_id(kword, cloud))

	def incr_counter_count(self, id):
		loc_cursor = self.conn.cursor()
		fetch = """update word_counter set count = count + 1 where _id = '%s'"""
		try:
			loc_cursor.execute(fetch, id)
			self.conn.commit()
		except:
			self.conn.rollback()
			raise Exception("Counter id doesnt exist")

	def fetch_counter_id(self, word, cloud):
		loc_cursor = self.conn.cursor()
		query = """select _id from word_counter where _keyword = '%s' and _cloud = '%s'"""
		try:
			loc_cursor.execute(query, (word, cloud))
			return loc_cursor.fetchall()[0][0]
		except:
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

	#START: Cloud Table

	def point_in_cloud(self, p_lat, p_lng, day, t, layer):
		fetch = self.help_point_in_cloud(t, day, layer)

		for coords in range(0, len(fetch)):
			id = fetch[coords][0]
			start_lat = "%20.15lf" % fetch[coords][1]
			start_lng = "%20.15lf" % fetch[coords][2]
			end_lat = "%20.15lf" % fetch[coords][3]
			end_lng = "%20.15lf" % fetch[coords][4]
			print "Fetched"
			if start_lng == p_lng:
				p_lng += self.EDGE
			if start_lat == p_lat:
				p_lat += self.EDGE
			if end_lng == p_lng:
				p_lng -= self.EDGE
			if end_lat == p_lat:
				p_lat -= self.EDGE
			if float(start_lng) < float(p_lng):
				if float(start_lat) > float(p_lat):
					if float(end_lng) > float(p_lng):
						if float(end_lat) < float(p_lat):
							print "point found"
							return id
						else:
							continue
					else:
						continue
				else:
					continue
			else:
				continue
		return 0

	def help_point_in_cloud(self, t, day, layer):
		loc_cursor = self.conn.cursor()
		list = []
		query = """select _id, start_lat, start_lng, end_lat, end_lng from cloud where start_time <= '%s' and end_time >= '%s' and day = '%s' and layer = '%s' """ % (
		t, t, day, layer)
		loc_cursor.execute(query)
		for each in loc_cursor.fetchall():
			list.append(each)
		return list

	def get_cloud_coords(self, id):
		local_cursor = self.conn.cursor()
		query = """select start_lat, start_lng, end_lat, end_lng from cloud where _id = '%s'""" % id
		local_cursor.execute(query)
		return local_cursor.fetchall()[0]

	def cloud_exists(self, s_lat, s_lng, e_lat, e_lng):
		loc_cursor = self.conn.cursor()
		query = """ select exists(select * from cloud where start_lat = '%s' and start_lng = '%s' and end_lat = '%s'
																	and end_lng = '%s');"""
		loc_cursor.execute(query, (s_lat, s_lng, e_lat, e_lng))
		return loc_cursor.fetchall()[0][0]

	def get_cloud_id(self, s_lat, s_lng, end_lat, end_lng, time, day, layer):
		loc_cursor = self.conn.cursor()
		output = []
		query = """select _id from cloud where start_lat = '%s' AND start_lng = '%s' and end_lat = '%s'
																		and end_lng = '%s' and time = %s and day = %s and layer = '%s'"""
		try:
			loc_cursor.execute(query, (s_lat, s_lng, end_lat, end_lng, time, day, layer))
		except:
			raise Exception("Cant fetch id - cloud doesnt exist")
		out = loc_cursor.fetchall()
		for i in range(0, 4):
			output.append(out[i][0])
		return output

	def layers(self, layer):
		loc_cursor = self.conn.cursor()
		def_time = time.strftime("12:00:00")
		def_day = "TUE"
		query = """select start_lat, start_lng, end_lat, end_lng from cloud where start_time = %s and day = %s and layer = '%s' """
		loc_cursor.execute(query, (def_time, def_day, layer))
		return loc_cursor.fetchall()

	def clouds_in_clouds(self, cloud_id):
		coords = self.get_cloud_coords(cloud_id) #[0] start_lat, [1] start_lng, [2] end_lat, [3] end_lng
		loc_cursor = self.conn.cursor()
		result = []
		def_time = time.strftime("12:00:00")
		def_day = "TUE"
		query = """select _id from cloud where
					start_lat >  '%s' and
					start_lat <= '%s' and
					start_lng <  '%s' and
					start_lng >= '%s' and
					end_lat   <  '%s' and
					end_lat	  >= '%s' and
					end_lng   >  '%s' and
					end_lng   <= '%s' and
					start_time =  %s  and
					day = %s;
					"""
		try:
			loc_cursor.execute(query, (coords[2], coords[0],coords[3],coords[1],coords[0],coords[2],coords[1],coords[3],
									   def_time, def_day))
		except:
			raise Exception("Cloud doesnt exist")
		out = loc_cursor.fetchall()
		for i in range(0,len(out)):
			result.append(out[i][0])
		return result

	def custom_cloud(self, start_lat, start_lng, end_lat,end_lng):  #useless atm - needs to be fixed
		#10000% private method - user MUST NOT be able to access it under any circumstances !
		loc_cursor = self.conn.cursor()
		if self.coord_in_grid(start_lat, start_lng):
			if self.coord_in_grid(end_lat,
								  end_lng):  # Some calculations will be needed in order to identify the actual position
				if float(start_lat) > float(end_lat):
					if float(start_lng) < float(end_lng):
						if self.cloud_exists(start_lat, start_lng, end_lat, end_lng) != 1:
							# All checks passed - can make a safe query now
							morning = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '4:00:00'),
																								  time.strftime(
																									  '11:59:59'))
							try:
								loc_cursor.execute(morning)
								self.conn.commit()
							except:
								self.conn.rollback()
							a_noon = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '12:00:00'),
																								  time.strftime(
																									  '16:59:59'))
							try:
								loc_cursor.execute(a_noon)
								self.conn.commit()
							except:
								self.conn.rollback()
							evenin = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '17:00:00'),
																								  time.strftime(
																									  '21:59:59'))
							try:
								loc_cursor.execute(evenin)
								self.conn.commit()
							except:
								self.conn.rollback()
							night = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
								VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (start_lat, start_lng,
																								  end_lat, end_lng,
																								  time.strftime(
																									  '22:00:00'),
																								  time.strftime(
																									  '3:59:59'))
							try:
								loc_cursor.execute(night)
								self.conn.commit()
							except:
								self.conn.rollback()

		return self.get_cloud_id(start_lat, start_lng, end_lat, end_lng)

	def get_layer_coords(self, layer):
		loc_cursor = self.conn.cursor()
		fetch = []
		if 0 <= layer < 5:
			query = """select start_lat, start_lng, end_lat, end_lng from cloud where layer = '%s'"""
			loc_cursor.execute(query, layer)
			fetch = loc_cursor.fetchall()
		return fetch

	def error_cloud(self):
		loc_cursor = self.conn.cursor()
		query = """insert into cloud """
	#END: Cloud Table

	# START: Tweet_keyword table
	def insert_twt_keyword(self, tweet_id, kword):
		loc_cursor = self.conn.cursor()
		query = """insert into tweet_keywords (_tweet, _keyword, date) values ('%s', '%s')"""
		try:
			loc_cursor.execute(query, (tweet_id, self.fetch_keyword_id(kword)))
			self.conn.commit()
		except:
			self.conn.rollback()

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

	def store_data(self, tweet):
		tweet = tweet.dict()
		text = self.elim_useless(tweet['text'])
		day = self.parse_timestamp(tweet['time'])
		cloud = []
		self.insert_keyword(text)
		for layer in range(0, 5):
			cloud.append(self.point_in_cloud(tweet['lat'], tweet['lng'], day[0], day[1], layer))
		if cloud[0] == 0:
			return 0

		for each in text:
			for c in cloud:
				self.insert_counter(self.fetch_keyword_id(each), c)

		for kw in text:
			self.insert_twt_keyword(tweet['_id'], kw)

		print "NExt tweet"

	def parse_timestamp(self, timestamp):  # 2016-06-07
		week_day = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
		timestamp = str(timestamp)
		year = int(timestamp[0:4])
		month = int(timestamp[5:7])
		day = int(timestamp[8:10])
		wk = week_day[datetime.date(year, month, day).weekday() + 1]
		t = time.strftime(timestamp[10:20])
		return wk, t


