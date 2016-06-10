from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect
from stop_words import get_stop_words
class Cloud_Parser(object):
	"""Parse collected data, retrieve keywords and store them"""
	
	def __init__(self):
		self.conn = Mysql_Connect().get_conn()
		self.cursor = self.conn.cursor()

		CREATE_KEYWORDS_TABLE = """CREATE TABLE IF NOT EXISTS keywords (
			_id BIGINT UNSIGNED NOT NULL AUTOINCREMENT,
			word CHAR(100) NOT NULL UNIQUE,
			PRIMARY KEY ( _id ),
			INDEX KEY ( keyword )
		)"""
		self.cursor.execute(CREATE_KEYWORDS_TABLE)

		CREATE_CLOUD_TABLE = """CREATE TABLE IF NOT EXISTS cloud (
			_id BIGINT UNSIGNED NOT NULL AUTOINCREMENT,
			start_lat DOUBLE(12,7),
			start_lng DOUBLE(12,7),
			end_lat DOUBLE(12,7),
			end_lng DOUBLE(12,7),
			start_at TIME,
			end_at,
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

	def elim_useless(self, list):
		stop_words = get_stop_words("en")
		for text in list:
			text = text.split()
			for word in text:
				if word in stop_words or len(word) <= 2:
					word = word.replace("")
		return list

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
		tweet = data.dict()
		tweet_text = tweet['text']
		global_text_data = self.elim_useless(tweet_text)
		for text in global_text_data:
			for word in text:
				feed = """INSERT IGNORE INTO keywords (word) VALUES (%s)""" % word
				try:
					self.cursor.execute(feed)
					self.conn.commit()
				except:
					self.conn.rollback()
		#END::global word cloud

		#START::location based wordclouds
		#Start coordinate - 53.45453140530998,-6.39404296875
		w,h = 141, 107
		grid = [[0 for x in range(w)]for y in range(h)]
		start_lng = 53.45698455620496
		for i in range(h):
			start_lat = -6.39404296875
			for j in range(w):
				grid[i][j] = {start_lat, start_lng}
				start_lat += 0.0022457775
			start_lng+=0.002457775
		#END::location based wordclouds
				#CREATE_CLOUD_TABLE = """CREATE TABLE IF NOT EXISTS cloud (
				#			_id BIGINT UNSIGNED NOT NULL AUTOINCREMENT,
				#			start_lat DOUBLE(12,7),
				#			start_lng DOUBLE(12,7),
				#			end_lat DOUBLE(12,7),
				#			end_lng DOUBLE(12,7),
				#			start_at TIME,
				#			end_at,
				#			PRIMARY KEY ( _id )
				#		)"""
		#update keywords table



		#update cloud_count table