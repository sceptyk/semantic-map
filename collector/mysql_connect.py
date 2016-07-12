import MySQLdb

class Mysql_Connect(object):

	conn = None

	def __init__(self, dev = False):
		if dev:
			self.conn = self.connect_dev()
		else:
			self.conn = self.connect()

	def connect(self):
		return MySQLdb.connect(
				host = '127.0.0.1',#'127.9.157.2',
				port = 3307,
				user = 'adminw9XD8Ju',
				passwd = 'QIY_8QyGMW-X', 
				db = 'ucd')

	def connect_dev(self):
		return MySQLdb.connect(
				host = '127.0.0.1',
				port = 3306,
				user = 'root',
				passwd = 'he110w0rld',
				db = 'scmp2016')

	def execute(self, sql):

		cursor = None

		try:
			cursor = self.conn.cursor()
			cursor.execute(sql)
			self.conn.commit()
		#except (AttributeError, MySQLdb.OperationalError):
		#	try:
		#		print("reconnected")
		#		self.conn = self.connect()
		#		cursor = self.conn.cursor()
		#		cursor.execute(sql)
		#		self.conn.commit()
		#	except:
		#		print("Exception on reconnect:")
		#		#print(sql)
		#		self.conn.rollback()
		except Exception, e:
		#	print("Exception on first try:")
		#	print(str(e))
			#print(sql)
			self.conn.rollback()

		return cursor

	def get_connection(self):
		return self.connect_dev()

	def get_cursor(self):
		return self.conn.cursor()

	def close(self):
		self.conn.close()