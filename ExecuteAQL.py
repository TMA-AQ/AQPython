import os, tempfile

class ExecuteAQL:

	#
	#
	def __init__(self, db_path, db_name):
		self.db_path = db_path
		self.db_name = db_name
		
	#
	# execute aql query and get results
	def execute(self, query, verbose=True):
		
		# save aql query and sql query
		aql_file = open('tmp.aql', 'w')
		aql_file.write(query)
		aql_file.close()
		
		# execute
		cmd_str = "AQEngineTests --queries \"" + aql_file.name + "\" --db-name=" + self.db_name + " --log-level=2 "
		if verbose:
			cmd_str += "--check-result -v > result.txt"
		os.system(cmd_str)
		
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
		
		return rows
		
		
	#
	# execute aq engine
	def run_aq_engine(self, ini_filename, ident, mode):
		cmd_str = 'E:/AQ_Bin/AQ_Engine.exe ' + ini_filename + ' ' + ident + ' ' + mode
		os.system(cmd_str)
		