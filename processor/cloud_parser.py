from model.tweet import Tweet
from collector.mysql_connect import Mysql_Connect

class Cloud_Parser(object){
	"""Parse collected data, retrieve keywords and store them"""
	
	def __init__:
		self.conn = Mysql_Connect().get_conn()
		self.cursor = self.conn.cursor()

		CREATE_KEYWORDS_TABLE = """CREATE TABLE `ucd`.`new_table` (
			  `_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
			  `word` CHAR(100) NOT NULL COMMENT '',
			  PRIMARY KEY (`_id`)  COMMENT '',
			  UNIQUE INDEX `word_UNIQUE` (`word` ASC)  COMMENT '');"""
		self.cursor.execute(CREATE_KEYWORDS_TABLE)

		CREATE_CLOUD_TABLE = """CREATE TABLE `ucd`.`new_table` (
			  `_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
			  `start_lat` DOUBLE(12,7) NULL COMMENT '',
			  `start_lng` DOUBLE(12,7) NULL COMMENT '',
			  `end_lat` DOUBLE(12,7) NULL COMMENT '',
			  `end_lng` DOUBLE(12,7) NULL COMMENT '',
			  `start_at` TIME NULL COMMENT '',
			  `end_at` TIME NULL COMMENT '',
			  PRIMARY KEY (`_id`)  COMMENT '');"""
		self.cursor.execute(CREATE_CLOUD_TABLE)

		CREATE_COUNTER_TABLE = """CREATE TABLE `ucd`.`word_counter` (
			  `_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
			  `_keyword` BIGINT UNSIGNED NOT NULL COMMENT '',
			  `_cloud` BIGINT UNSIGNED NOT NULL COMMENT '',
			  `count` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '',
			  PRIMARY KEY (`_id`)  COMMENT '',
			  INDEX `keywrod_idx` (`_keyword` ASC)  COMMENT '',
			  INDEX `cloud_idx` (`_cloud` ASC)  COMMENT '',
			  CONSTRAINT `keyword`
			    FOREIGN KEY (`_keyword`)
			    REFERENCES `ucd`.`keywords` (`_id`)
			    ON DELETE NO ACTION
			    ON UPDATE NO ACTION,
			  CONSTRAINT `cloud`
			    FOREIGN KEY (`_cloud`)
			    REFERENCES `ucd`.`cloud` (`_id`)
			    ON DELETE NO ACTION
			    ON UPDATE NO ACTION);"""
		self.cursor.execute(CREATE_COUNTER_TABLE)

		CREATE_TWEET_KEYWORDS_TABLE = """CREATE TABLE `ucd`.`tweet_keywords` (
			  `_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
			  `_tweet` BIGINT UNSIGNED NOT NULL COMMENT '',
			  `_keyword` BIGINT UNSIGNED NOT NULL COMMENT '',
			  PRIMARY KEY (`_id`)  COMMENT '',
			  INDEX `tweet_idx` (`_tweet` ASC)  COMMENT '',
			  INDEX `keyword_idx` (`_keyword` ASC)  COMMENT '',
			  CONSTRAINT `tweet`
			    FOREIGN KEY (`_tweet`)
			    REFERENCES `ucd`.`tweets` (`_id`)
			    ON DELETE NO ACTION
			    ON UPDATE NO ACTION,
			  CONSTRAINT `keyword_tweet`
			    FOREIGN KEY (`_keyword`)
			    REFERENCES `ucd`.`keywords` (`_id`)
			    ON DELETE NO ACTION
			    ON UPDATE NO ACTION);"""
	
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
		    
		tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
		emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
		 
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
				cursor.execute(sql)
				results = cursor.fetchall()
				for row in results:
					tweet = Tweet()
					tweet.populate(row)

					self.store_data(tweet)

				start = end
				end += chunk_size
			except:
				print "Error: unable to fecth data"
				break

	
	def store_data(self, data):
		pass
		#TODO update each branch
		# -- each place based on location
		# -- each time part
		# -- global list
		# 
		
		# update cloud table
		# add locations clouds
		
		# add time clouds
		
		#update keywords table
		sql = """INSERT IGNORE INTO keywords (word) 
			VALUES (%s)""" % keyword

		try:
			self.cursor.execute(sql)
			self.conn.commit()
		except:
			self.conn.rollback()

		#update cloud_count table

}