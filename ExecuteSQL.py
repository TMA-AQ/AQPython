import time

try:
	import MySQLdb as mdb
except:
	print 'no module MySQLdb found'
	
try:
	import cx_Oracle as orcl
except:
	print 'no module cx_Oracle found'
	
class ExecuteSQL:
	
	#
	#
	def __init__(self, sgbd, host, user, password, db_name):
		self.sgbd = sgbd
		self.host = host
		self.user = user
		self.password = password
		self.db_name = db_name
		
		try:
			if self.sgbd == 'Oracle':
				con_str = user + '/' + password + '@' + host + '/xe' # FIXME
				self.con = orcl.connect(con_str);
			elif self.sgbd == 'MySQL':
				self.con = mdb.connect(self.host, self.user, self.password, self.db_name)
			else:
				raise Exception('sgbd ' + self.sgbd + ' not supported')
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
	#
	def get_tables(self):
		if self.sgbd == 'Oracle':
			self.cur.execute('select table_name from user_tables')
		elif self.sgbd == 'MySQL':
			self.cur.execute('show tables')
		return self.cur.fetchall()
	
	#
	#
	def get_description(self, table_name):
		desc = []
		if self.sgbd == 'Oracle':
			self.cur.execute('select column_name, data_type, data_length from all_tab_columns where table_name = \'' + table_name + '\'')
			for c in self.cur.fetchall():
				cname = c[0]
				ctype = c[1]
				csize = 1
				if ctype == 'NUMBER':
					ctype = 'INT'
				elif ctype == 'FLOAT':
					ctype = 'FLOAT'
				elif ctype == 'VARCHAR2':
					ctype = 'VARCHAR'
					csize = c[2]
				else:
					print ctype
					raise 'type', ctype, 'not supported' # FIXME
				desc.append({'name':cname, 'size':csize, 'type':ctype})
		elif self.sgbd == 'MySQL':
			self.cur.execute('desc ' + table_name)
			for c in self.cur.fetchall():
				cname = c[0]
				ctype = c[1]
				csize = 1
				if ctype[0:7].lower() == 'varchar':
					p, e = ctype.find('('), ctype.find(')')
					csize = int(ctype[p+1:e])
				if '(' in ctype:
					ctype = ctype[0:ctype.find('(')]
				desc.append({'name':cname, 'size':csize, 'type':ctype})
		return desc
		
	#
	# execute sql query	and get results		
	def execute_and_commit(self, query):	
		try:
			self.cur.execute(query)
			self.con.commit()
		except:
			print 'error executing \'' + query + '\''
			
	#
	# execute sql query	and get results		
	def execute_and_fetch(self, query):
		t = time.time()
		self.cur.execute(query)
		rows = [ r for r in self.cur.fetchall() ]
		t = time.time() - t
		return (0, t, rows)

if __name__ == '__main__':
	exec_sql = ExecuteSQL('Oracle', 'localhost', 'tma', 'AlgoQuest', 'algoquest')

	for table_name in ['t1', 't2', 't3']:
		exec_sql.execute_and_commit('drop table ' + table_name)
		exec_sql.execute_and_commit('create table ' + table_name + ' (id int primary key, v1 int not null)')	
		for i in range(1, 11):
			exec_sql.execute_and_commit('insert into ' + table_name + ' (id, v1) values (' + str(i) + ',' + str(i) + ')')
	
	rc, t, rows1 = exec_sql.execute_and_fetch('select * from t1')
	rc, t, rows2 = exec_sql.execute_and_fetch('select * from t2')
	print rows1
	print rows2
	
	rc, t, rows = exec_sql.execute_and_fetch('select * from t1 full outer join t2 on t1.v1 = t2.v1')
	for row in rows:
		print row