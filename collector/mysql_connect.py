import MySQLdb

class Mysql_Connect(object):

	conn = None

	def __init__(self, dev = False):
		if dev:
			Mysql_Connect.conn = self.connect_dev()
		else:
			Mysql_Connect.conn = self.connect()

	def connect(self):
		return MySQLdb.connect(
				host = '127.0.0.1',#'127.9.157.2',
				port = 3306,
				user = 'adminw9XD8Ju',
				passwd = 'QIY_8QyGMW-X', 
				db = 'ucd')

	def connect_dev(self):
		return MySQLdb.connect(
				host = '127.0.0.1',
				port = 3306,
				user = 'root',
				passwd = '', 
				db = 'semantic_map_2016')

	def execute(self, sql):

		cursor = None

		try:
			cursor = Mysql_Connect.conn.cursor()
			cursor.execute(sql)
			Mysql_Connect.conn.commit()
		except (AttributeError, MySQLdb.OperationalError):
			try:
				print("reconnected")
				Mysql_Connect.conn = self.connect()
				cursor = Mysql_Connect.conn.cursor()
				cursor.execute(sql)
				Mysql_Connect.conn.commit()
			except:
				print(sql)
				Mysql_Connect.conn.rollback()
		except Exception, e:
			print(str(e))
			print(sql)
			Mysql_Connect.conn.rollback()

		return cursor

	def get_connection(self):
		return Mysql_Connect.conn

	def get_cursor(self):
		return Mysql_Connect.conn.cursor()

	def close(self):
		Mysql_Connect.conn.close()
