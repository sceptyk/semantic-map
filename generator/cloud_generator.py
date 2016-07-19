import math
from collector.mysql_connect import Mysql_Connect
import time
import Geohash as geo

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
        start_lat = 53.50152546260909
        for i in range(self.size_h):
            loc_rix.append([])
            start_lng = -6.391296386718749
            for j in range(self.size_w):
                loc_rix[i].append((start_lat, start_lng))
                start_lng += 0.0022580788
            start_lat -= 0.00224643929
        print "Matrix obtained"
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

        CREATE_CLOUD_TABLE = """CREATE TABLE  cloud  (
    		               _id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '',
    		               start_crds  CHAR(20) NULL COMMENT '',
    		               end_crds  CHAR(20) NULL COMMENT '',
    		               layer INT(1) NULL COMMENT '',
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
    					   time_index BIGINT NOT NULL COMMENT '',
    					   day CHAR(3) NOT NULL COMMENT '',
    					   count  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '',
    					  PRIMARY KEY ( _id )  COMMENT '',
    					  INDEX  keyword_idx  ( _keyword  ASC)  COMMENT '',
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
        layers = [0,2,4]
        for j in layers:
            self.insert_layer(j)

    def insert_layer(self, layer):  # 5 layers - 0 to 4 (timestamp - time.strftime('22:00:00'))
        loc_cursor = self.conn.cursor()
        itr = int(math.pow(2, layer))
        query = """insert into cloud (start_crds, end_crds, layer)
            values (%s,%s,'%s')"""
        values = []
        for i in range(0, self.size_h - itr, itr):
            for j in range(0, self.size_w - itr, itr):
                values.append((geo.encode(self.Matrix[i][j][0], self.Matrix[i][j][1], 20), geo.encode(self.Matrix[i + itr][j + itr][0],
                                self.Matrix[i + itr][j + itr][1],20),layer))
        try:
            loc_cursor.executemany(query,values)
            self.conn.commit()
        except:
            print "insert layer"
            self.conn.rollback()

