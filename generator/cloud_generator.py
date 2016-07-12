import math
from collector.mysql_connect import Mysql_Connect
import time

class Cloud_Generator(object):
    def __init__(self,x,y):
        self.size_h = y
        self.size_w = x
        self.conn = Mysql_Connect().get_connection()
        self.Matrix = self.get_coords()
        self.init_glob_cloud()
        #self.populate_clouds()

    def get_coords(self):
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

    def rlat(self, deg):
        return (deg * math.pi) / 180

    def metres_per_lat(self, rlat):
        return 111132.92 - 559.82 * math.cos(2 * rlat)

    def rlng(self, deg):
        return (50 * math.pi) / 180

    def metres_per_lng(self, rlng):
        return 111412.84 * math.cos(rlng) - 93.5 * math.cos(3 * rlng)

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

        CREATE_POINT_TABLE = """CREATE TABLE IF NOT EXISTS tweet_location (
    		            _id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    		            lat DOUBLE(12, 7),
    		            lng DOUBLE(12, 7),
    		            _keyword BIGINT,
    		            PRIMARY KEY ( _id )
    		        )"""
        try:
            loc_cursor.execute(CREATE_POINT_TABLE)
        except:
            self.conn.rollback()
        CREATE_CLOUD_TABLE = """CREATE TABLE  cloud  (
    		               _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    		               start_lat  DOUBLE(12,7) NULL COMMENT '',
    		               start_lng  DOUBLE(12,7) NULL COMMENT '',
    		               end_lat  DOUBLE(12,7) NULL COMMENT '',
    		               end_lng  DOUBLE(12,7) NULL COMMENT '',
    		               start_time  TIME NULL COMMENT '',
    		               end_time  TIME NULL COMMENT '',
    		               layer INT(1) NULL COMMENT '',
    		               day CHAR(3) NULL COMMENT '',
    		              PRIMARY KEY ( _id )  COMMENT '');"""
        try:
            loc_cursor.execute(CREATE_CLOUD_TABLE)
            self.conn.commit()
        except:
            self.conn.rollback()

        CREATE_COUNTER_TABLE = """CREATE TABLE  word_counter  (
    					   _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    					   _keyword  BIGINT UNSIGNED NOT NULL COMMENT '',
    					   _cloud  BIGINT UNSIGNED NOT NULL COMMENT '',
    					   count  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '',
    					  PRIMARY KEY ( _id )  COMMENT '',
    					  INDEX  keywrod_idx  ( _keyword  ASC)  COMMENT '',
    					  INDEX  cloud_idx  ( _cloud  ASC)  COMMENT '',
    					  CONSTRAINT  keyword
    						FOREIGN KEY ( _keyword )
    						REFERENCES  keywords  ( _id )
    						ON DELETE NO ACTION
    						ON UPDATE NO ACTION,
    					  CONSTRAINT  cloud
    						FOREIGN KEY (_cloud)
    						REFERENCES cloud  ( _id )
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
    		              PRIMARY KEY ( _id )  COMMENT '',
    		              INDEX  tweet_idx  ( _tweet  ASC)  COMMENT '',
    		              INDEX  keyword_idx  ( _keyword  ASC)  COMMENT '',
    		              CONSTRAINT  tweet
    		                FOREIGN KEY ( _tweet )
    		                REFERENCES  tweets  ( _id )
    		                ON DELETE NO ACTION
    		                ON UPDATE NO ACTION,
    		              CONSTRAINT  keyword_tweet
    		                FOREIGN KEY ( _keyword )
    		                REFERENCES keywords  ( _id )
    		                ON DELETE NO ACTION
    		                ON UPDATE NO ACTION);"""

        try:
            loc_cursor.execute(CREATE_TWEET_KEYWORDS_TABLE)
        except:
            self.conn.rollback()

    def populate_clouds(self):
        days = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
        for j in range(0, 5):
            for i in days:
                self.insert_layer(j, time.strftime('4:00:00'), time.strftime('11:59:59'), i)
                self.insert_layer(j, time.strftime('12:00:00'), time.strftime('16:59:59'), i)
                self.insert_layer(j, time.strftime('17:00:00'), time.strftime('21:59:59'), i)
                self.insert_layer(j, time.strftime('22:00:00'), time.strftime('3:59:59'), i)

    def insert_layer(self, layer, s_time, e_time, day):  # 5 layers - 0 to 4 (timestamp - time.strftime('22:00:00'))
        loc_cursor = self.conn.cursor()
        itr = int(math.pow(2, layer))
        count = 0
        query = """insert into cloud (start_lat, start_lng, end_lat, end_lng, start_time, end_time, layer, day)
            values ('%s','%s','%s','%s', %s, %s, '%s', %s)"""
        values = []
        for i in range(0, self.size_h - itr, itr):
            for j in range(0, self.size_w - itr, itr):
                values.append((self.Matrix[i][j][0], self.Matrix[i][j][1], self.Matrix[i + itr][j + itr][0],
                                self.Matrix[i + itr][j + itr][1], s_time, e_time, layer, day))
        try:
            loc_cursor.executemany(query,values)
            self.conn.commit()
        except:
            self.conn.rollback()
