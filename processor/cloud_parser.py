from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
from stop_words import get_stop_words
import time
import string


class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	
	def __init__(self):
		self.conn = Mysql_Connect().get_conn()
		self.cursor = self.conn.cursor()
		self.Matrix = self.get_grid()
		CREATE_KEYWORDS_TABLE = """CREATE TABLE IF NOT EXISTS keywords (
			_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			word CHAR(100) NOT NULL UNIQUE,
			PRIMARY KEY ( _id ),
			INDEX KEY ( keyword )
		)"""
		self.cursor.execute(CREATE_KEYWORDS_TABLE)

		CREATE_CLOUD_TABLE = """CREATE TABLE IF NOT EXISTS cloud (
			_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			start_lat DOUBLE(12,15),
			start_lng DOUBLE(12,15),
			end_lat DOUBLE(12,15),
			end_lng DOUBLE(12,15),
			start_time TIME,
			end_time TIME,
			PRIMARY KEY ( _id )
		)"""
		self.cursor.execute(CREATE_CLOUD_TABLE)

		CREATE_COUNTER_TABLE = """CREATE TABLE IF NOT EXISTS word_counter (
			_id BIGINT UNSIGNED NOT NULL AUTOINCREMENT,
			_keyword BIGINT UNSIGNED NOT NULL,
			_cloud BIGINT UNSIGNED NOT NULL,
			count BIGINT UNSIGNED NOT NULL,
			PRIMARY KEY ( _id )
		)"""
		self.cursor.execute(CREATE_COUNTER_TABLE)
	
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
		
		chunk_size = 1000

		while True:
			start = 0
			end = chunk_size
			sql = """SELECT * FROM tweets ORDER BY _id LIMIT %d, %d""" % (start, end)

			try:
				self.cursor.execute(sql)
				results = self.cursor.fetchall()
				for row in results:
					tweet = Tweet()
					tweet.populate(row)

					self.store_data(tweet)

				start = end
				end += chunk_size
			except:
				print "Error: unable to fecth data"
				break

	def get_grid(self):
		size_w, size_h = 141, 107
		loc_rix = []
		start_lat = 53.45698455620496
		for i in range(size_h):
			loc_rix.append([])
			start_lng = -6.39404296875
			for j in range(size_w):
				loc_rix[i].append((start_lat, start_lng))
				start_lng += 0.0022457775
			start_lat -= 0.0022457775
		return loc_rix

	def elim_useless(self, txt):
		list = txt.split(' ')
		r_list = []
		for word in list:
			if word.lower() in self.stopwords() or len(word) <= 2:
				continue
			else:
				r_list.append(word)
		return r_list

	def populate_clouds(self):
		#self.reset_increment()
		size_w, size_h = 141, 107
		for i in range(size_h - 1):
			for j in range(size_w - 1):
				# Morning tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('4:00:00'), time.strftime('11:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()
				# Afternoon tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('12:00:00'), time.strftime('16:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()
				# Evening tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('17:00:00'), time.strftime('21:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()
				# Night time tweets
				pop_q = """INSERT INTO cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time)
									VALUES (%20.15lf, %20.15lf, %20.15lf, %20.15lf, '%s', '%s')""" % (
					self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + 1][j + 1][0],
					self.Matrix[i + 1][j + 1][1], time.strftime('22:00:00'), time.strftime('3:59:59'))
				try:
					self.cursor.execute(pop_q)
					self.conn.commit()
				except:
					self.conn.rollback()

	def reset_increment(self):
		clear = """truncate table cloud"""
		self.cursor.execute(clear)

	def get_clouds_count(self):
		count_query = """select count(*) from cloud;"""
		loc_cursor = self.conn.cursor()
		loc_cursor.execute(count_query)
		return loc_cursor.fetchall()

	def point_in_cloud(self, p_lat, p_lng):
		EDGE = 0.00000000001
		loc_cursor = self.conn.cursor()
		# q = """select start_lat from cloud where _id=%d;"""
		for id in range(1, self.get_clouds_count()):
			start_latQ = """select start_lat from cloud where _id=%d;""" % id
			loc_cursor.execute(start_latQ)
			start_lat = "%20.15lf" % loc_cursor.fetchone()[0]

			start_lngQ = """select start_lng from cloud where _id=%d;""" % id
			loc_cursor.execute(start_lngQ)
			start_lng = "%20.15lf" % loc_cursor.fetchone()[0]

			end_latQ = """select end_lat from cloud where _id=%d;""" % id
			loc_cursor.execute(end_latQ)
			end_lat = "%20.15lf" % loc_cursor.fetchone()[0]

			end_lngQ = """select end_lng from cloud where _id=%d;""" % id
			loc_cursor.execute(end_lngQ)
			end_lng = "%20.15lf" % loc_cursor.fetchone()[0]
			if start_lng == p_lng:
				p_lng += EDGE
			if start_lat == p_lat:
				p_lat += EDGE
			if end_lng == p_lng:
				p_lng -= EDGE
			if end_lat == p_lat:
				p_lat -= EDGE
			if float(start_lng) < float(p_lng):
				if float(start_lat) > float(p_lat):
					if float(end_lng) > float(p_lng):
						if float(end_lat) < float(p_lat):
							return id
						else:
							continue
					else:
						continue
				else:
					continue
			else:
				continue
		return "Not found"

	def get_cloud_coords(self, id):
		local_cursor = self.conn.cursor()
		query = "select * from cloud where _id=%d" % id
		local_cursor.execute(query)
		row = local_cursor.fetchall()[0]
		return row

	def glob_cloud(self, tweet):
		parse_tweet = tweet.dict()
		tweet_text = parse_tweet['text']
		global_text_data = self.elim_useless(tweet_text)
		clear_global_text_data = self.clear_punctuation(global_text_data)
		for text in clear_global_text_data:
			for word in text:
				feed = """INSERT IGNORE INTO keywords (word) VALUES (%s)""" % word
				try:
					self.cursor.execute(feed)
					self.conn.commit()
				except:
					self.conn.rollback()

	def clear_punctuation(self, text):
		clear_string = ""
		delimiters = string.punctuation + "\n\t"
		for symbol in text:
			if symbol not in delimiters:
				clear_string += symbol
		return clear_string


	def store_data(self, data):
		pass
		#TODO update each branch
		# -- each place based on location
		# -- each time part
		# -- global list (done)
		# 
		
		# update cloud table
		# add locations clouds
		
		# add time clouds
		#START::global word cloud
		#take text out

		#END::global word cloud

		#START::location based wordclouds
		#Dublin divided into 250m*250m squares in total of 14840 squares
		#END::location based wordclouds
		#update keywords table


		#update cloud_count table
	def stopwords(self):
		with open("stopwords.txt") as input:
			text = input.readline()
		list = text.split(",")
		clear_list = []
		for word in list:
			clear_list.append(word.replace(" ", ""))
		return clear_list