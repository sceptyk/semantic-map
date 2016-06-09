import MySQLdb

class Mysql_Connect(object):

	conn = None

	def __init__(self):

		if Mysql_Connect.conn is None:
			Mysql_Connect.conn = MySQLdb.connect(
				host = '127.9.157.2',
				port = 3306,
				user = 'adminw9XD8Ju',
				passwd = 'QIY_8QyGMW-X', 
				db = 'ucd')

	def get_conn(self):
		return Mysql_Connect.conn