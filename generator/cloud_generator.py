from collector.mysql_connect import Mysql_Connect

class Cloud_Generator(object):
    def __init__(self):
        self.conn = Mysql_Connect().get_connection()
        self.init_glob_cloud()



    def init_glob_cloud(self):
        loc_cursor = self.conn.cursor()
        CREATE_KEYWORDS_TABLE = """CREATE TABLE keywords (
    		              _id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    		              word CHAR(100) NOT NULL COMMENT '',
    		              PRIMARY KEY (_id)  COMMENT '',
    		              UNIQUE INDEX word_UNIQUE (word ASC)  COMMENT '');"""
        try:
            loc_cursor.execute(CREATE_KEYWORDS_TABLE)
        except:
            self.conn.rollback()

        CREATE_CLOUD_TABLE = """CREATE TABLE  cloud  (
    		               _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    		               cloud  CHAR(20) NULL COMMENT '',
    		               layer INT(1) NULL COMMENT '',
    		              PRIMARY KEY ( _id )  COMMENT '',
    		              UNIQUE INDEX cloud_idx (cloud ASC) COMMENT '');"""

        try:
            loc_cursor.execute(CREATE_CLOUD_TABLE)
            self.conn.commit()
        except:
            self.conn.rollback()

        CREATE_COUNTER_TABLE = """CREATE TABLE  word_counter  (
    					   _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    					   _keyword  BIGINT UNSIGNED NOT NULL COMMENT '',
    					   _cloud  CHAR(10) NOT NULL COMMENT '',
    					   layer INT(1) NOT NULL COMMENT '',
    					   time_index BIGINT NOT NULL COMMENT '',
    					   day CHAR(3) NOT NULL COMMENT '',
    					   count  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '',
    					  PRIMARY KEY ( _id )  COMMENT '',
    					  UNIQUE INDEX  keyword_idx  ( _keyword  ASC)  COMMENT '',
    					  UNIQUE INDEX  cloud_idx  ( _cloud  ASC)  COMMENT '',
    					  UNIQUE INDEX layer_idx ( layer ASC) COMMENT '',
    					  CONSTRAINT  keyword
    						FOREIGN KEY ( _keyword )
    						REFERENCES  keywords  ( _id )
    						ON DELETE NO ACTION
    						ON UPDATE NO ACTION);"""
        try:
            loc_cursor.execute(CREATE_COUNTER_TABLE)
        except:
            self.conn.rollback()

        CREATE_TWEET_KEYWORDS_TABLE = """CREATE TABLE tweet_keywords  (
    		               _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    		               _tweet  BIGINT UNSIGNED NOT NULL COMMENT '',
    		               _keyword  BIGINT UNSIGNED NOT NULL COMMENT '',
    		              PRIMARY KEY ( _id )  COMMENT '');"""
        try:
            loc_cursor.execute(CREATE_TWEET_KEYWORDS_TABLE)
        except:
            self.conn.rollback()




