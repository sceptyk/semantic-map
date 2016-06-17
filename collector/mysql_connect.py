import MySQLdb

class Mysql_Connect(object):

	conn = None

	def __init__(self):
		Mysql_Connect.conn = self.connect()

	def connect(self):
		return MySQLdb.connect(
				host = '127.9.157.2', #'127.0.0.1'
				port = 3306,
				user = 'adminw9XD8Ju',
				passwd = 'QIY_8QyGMW-X', 
				db = 'ucd')

	def execute(self, sql):
		try:
			Mysql_Connect.conn.cursor().execute(sql)
			Mysql_Connect.conn.commit()
		except (AttributeError, MySQLdb.OperationalError):
			try:
				print("reconnected")
				Mysql_Connect.conn = self.connect()
				Mysql_Connect.conn.cursor().execute(sql)
				self.commit()
			except:
				print(sql)
				Mysql_Connect.conn.rollback()
		except Exception, e:
			print(str(e))
			print(sql)
			Mysql_Connect.conn.rollback()

	def get_connection():
		return Mysql_Connect.conn

	def get_cursor():
		return Mysql_Connect.conn.cursor()

	def close(self):
		Mysql_Connect.conn.close()
