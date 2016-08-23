from collector.mysql_connect import Mysql_Connect

class Cloud_Generator(object):
    def __init__(self):
        self.conn = Mysql_Connect()
        self.init_glob_cloud()
        print "Tables created"

    def init_glob_cloud(self):
        CREATE_KEYWORDS_TABLE = """CREATE TABLE IF NOT EXISTS keywords (
    		              _id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    		              word CHAR(100) NOT NULL COMMENT '',
    		              PRIMARY KEY (_id)  COMMENT '',
    		              UNIQUE INDEX word_UNIQUE (word ASC)  COMMENT '');"""
        
        self.conn.execute(CREATE_KEYWORDS_TABLE)

        CREATE_COUNTER_TABLE = """CREATE TABLE IF NOT EXISTS word_counter  (
    					   _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    					   _keyword  BIGINT UNSIGNED NOT NULL COMMENT '',
    					   _cloud  CHAR(10) NOT NULL COMMENT '',
                           _parent CHAR(10) NOT NULL COMMENT '',
                           _layer INT(2) NOT NULL COMMENT '',
    					   day_time INT(2) NOT NULL COMMENT '',
    					   day INT(2) NOT NULL COMMENT '',
    					   count  BIGINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '',
    					  PRIMARY KEY ( _id )  COMMENT '',
    					  UNIQUE KEY entry_unq (_keyword, _cloud, _layer, day_time, day)  COMMENT '',
                          INDEX KEY cloud_idx (_cloud) COMMENT '',
    					  CONSTRAINT  keyword
    						FOREIGN KEY ( _keyword )
    						REFERENCES  keywords  ( _id )
    						ON DELETE NO ACTION
    						ON UPDATE NO ACTION);"""
        
        self.conn.execute(CREATE_COUNTER_TABLE)

        CREATE_TWEET_KEYWORDS_TABLE = """CREATE TABLE IF NOT EXISTS tweet_keywords  (
    		               _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    		               _tweet  BIGINT UNSIGNED NOT NULL COMMENT '',
    		               _keyword  BIGINT UNSIGNED NOT NULL COMMENT '',
    		              PRIMARY KEY ( _id )  COMMENT '',
    		              CONSTRAINT kword
    		                FOREIGN KEY ( _keyword )
    		                REFERENCES keywords ( _id )
    		                ON DELETE NO ACTION,
    		              CONSTRAINT twt
    		                FOREIGN KEY ( _tweet )
    		                REFERENCES tweets ( _id )
    		                ON DELETE NO ACTION);"""
                            
        self.conn.execute(CREATE_TWEET_KEYWORDS_TABLE)