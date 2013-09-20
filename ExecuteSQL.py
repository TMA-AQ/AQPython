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
			self.con = None
			print "Error: %d: %s" % (e.args[0], e.args[1])
	
	#
	#
	def __del__(self):
		if not self.con is None:
			self.con.close()
		
	#
	# execute sql query	and get results		
	def execute_and_commit(self, query):		
		self.cur.execute(query)
		self.con.commit()

	#
	# execute sql query	and get results		
	def execute_and_fetch(self, query):		
		self.cur.execute(query)
		rows = self.cur.fetchall()
		return rows
