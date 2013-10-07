import sys, os, time, tempfile
import AlgoQuestDB as aq

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
			
		t = time.time()	
		rows = aq.Execute(self.cfg, query)
		t = time.time() - t
		rc = 0 # TODO
		return (rc, t, rows[1:])
		
		# save aql query and sql query
		aql_file = open('tmp.aql', 'w')
		aql_file.write(query)
		aql_file.close()
		
		# execute
		cmd_str = self.aq_engine_tests + " --queries \"" + aql_file.name + "\"" 
		cmd_str += " --root-path=" + self.db_path
		cmd_str += " --db-name=" + self.db_name 
		cmd_str += " --aq-engine=" + self.aq_engine
		cmd_str += " --display --log-level=2 > result.txt "
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
		
