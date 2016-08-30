import numpy as np
from collector.mysql_connect import Mysql_Connect

class Activity_Parser(object):
	

	def __init__(self):
		#get db connection
		db = Mysql_Connect()

		#select number of keywords
		select_keywords_size = """SELECT count(*) FROM keywords"""
		cursor = db.execute(select_keywords_size)
		keywords_size = fetchone()[0]

		#select clouds
		select_clouds = """SELECT 
			    _cloud
			FROM
			    word_counter
			WHERE
			    _keyword <> 1 AND _layer = 1
			GROUP BY _cloud"""

		select_keywords = """SELECT 
			    k.word, SUM(wc.count)
			FROM
				keywords k
			    INNER JOIN word_counter wc
			    ON wc._keyword = k._id
			WHERE
			    wc._keyword <> 1 AND _cloud = '%s'
			GROUP BY _keyword"""

		cursor = db.execute(select_clouds)
		results = cursor.fetchall()
		clouds = []
		for row in results:
			clouds.append(row[0])
		samples = []

		for cloud in clouds:
			#for each cloud select keywords
			cursor = db.execute(select_keywords % cloud)
			keywords = cursor.fetchall()

		#set rest of keywords 0
		
		#run clusterin
		
		#save data