import sys, os, time, tempfile
try:
	import AlgoQuestDB as aq
except:
	None

sys.path.insert(0, 'E:/AlgoQuest_DEV/AQSuite/x64/Debug')

class ExecuteAQL:

	#
	#
	def __init__(self, aq_engine_tests, db_path, db_name, aq_engine):
		self.aq_engine_tests = aq_engine_tests
		self.db_path = db_path
		self.db_name = db_name
		self.aq_engine = aq_engine
		
		self.cfg = aq.Settings()
		self.cfg.dbPath = db_path + '/' + db_name + '/'
		self.cfg.engine = aq_engine
		self.cfg.ident = 'test_aq_engine'
		self.cfg.index = False
		self.cfg.count = False
		
	#
	# execute aql query and get results
	def execute(self, query, verbose=True):
			
		rc = 0
		if aq != None:
			try:
				aq.SetLogLevel(2)
				t = time.time()	
				rows = aq.Execute(self.cfg, query)
				t = time.time() - t
				rc, t, rows = rc, t, rows[1:]
			except:
				print 'ERROR' 
				rc, t, rows = 1, 0, []
		else:
					
			# save aql query and sql query
			aql_file = open('tmp.aql', 'w')
			aql_file.write(query)
			aql_file.close()
			
			# execute
			cmd_str = self.aq_engine_tests + " --queries \"" + aql_file.name + "\"" 
			cmd_str += " --root-path=" + self.db_path
			cmd_str += " --db-name=" + self.db_name 
			cmd_str += " --aq-engine=" + self.aq_engine
			cmd_str += " --display --log-level=2 --force > result.txt "
			# print cmd_str
			t = time.time()
			rc = os.system(cmd_str)
			t = time.time() - t
			
			# get results as rows
			f = open('result.txt', 'r')
			f.readline()
			rows = []
			for line in f:
				row = line.split(';')
				row.pop()
				for i in range(len(row)):
					row[i] = row[i].strip()
				if row:
					rows.append(row)
			f.close()

		return (rc, t, rows)
		
		
	#
	# execute aq engine
	def run_aq_engine(self, ini_filename, ident, mode):
		cmd_str = self.aq_engine + ' ' + ini_filename + ' ' + ident + ' ' + mode
		t = time.time()
		rc = os.system(cmd_str)
		t = time.time() - t
		return (rc, t)
		
#
#
if __name__ == '__main__':
	
	aq.SetLogLevel(2)
	
	query  = ' SELECT , . T1 ID . T2 ID\n'
	query += ' FROM , T1 T2 \n'
	query += ' WHERE K_JEQ K_INNER . T2 ID K_INNER . T1 ID \n'
 
	exec_aql = ExecuteAQL('aq-engine-tests', 'e:/AQ_DATABASES/DB/', 'algoquest', 'aq-engine')
	r, t, rows = exec_aql.execute(query)
	print len(rows)
	for row in rows:
		for v in row:
			print v,
		print ''