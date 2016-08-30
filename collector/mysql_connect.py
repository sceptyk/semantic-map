import MySQLdb

class Mysql_Connect(object):

	def __init__(self, dev = False):
		self.conn = self.connect(dev)

	def connect(self, dev = False):
		if dev:
			return MySQLdb.connect(
				host = '127.0.0.1',
				port = 3306,
				user = 'root',
				passwd = '', 
				db = 'semantic_map_2016')
		else:
			return MySQLdb.connect(
					host = '127.0.0.1',
					port = 3306,
					user = 'username',
					passwd = 'password', 
					db = 'database_name')

	def execute(self, sql):

		cursor = None

		try:
			cursor = self.conn.cursor()
			cursor.execute(sql)
			self.conn.commit()
		except (AttributeError, MySQLdb.OperationalError) as e:
			print "reconnected: ", str(e)
			try:
				self.conn = self.connect()
				cursor = self.conn.cursor()
				cursor.execute(sql)
				self.conn.commit()
			except Exception, e:
				print("Exception on reconnect:")
				print(str(e))
				print(sql)
				self.conn.rollback()
		except Exception, e:
			print "Exception on first try:", str(e)
			print(sql)
			self.conn.rollback()

		return cursor

	def get_connection(self):
		return self.conn

	def get_cursor(self):
		return self.conn.cursor()

	def close(self):
		self.conn.close()
