import MySQLdb as mdb

class ExecuteSQL:
	
	#
	#
	def __init__(self, host, user, password, db_name):
		self.host = host
		self.user = user
		self.password = password
		self.db_name = db_name
		
		try:
			self.con = mdb.connect(self.host, self.user, self.password, self.db_name)
			self.cur = self.con.cursor()
		except mdb.Error, e:
			print "Error: %d: %s" % (e.args[0], e.args[1])
	
	#
	#
	def __del__(self):
		if self.con:
			self.con.close()
		
	#
	# execute sql query	and get results		
	def execute(self, query):		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		return rows
