import os, re, tempfile

#
#
def permut(values):
	if len(values) == 1:
		return [values]
	res = []
	for v in values:
		for p in permut([i for i in values if i != v]):
			res.append([v] + p)
	return res
		
#
# transform aql query into sql
def aql2sql(aql_query):

	sql_file = tempfile.NamedTemporaryFile('r')
	tmp_filename = sql_file.name
	sql_file.close()
	cmd_str = "echo " + aql_query.replace('\n', ' ') + " | AQL2SQL > " + tmp_filename
	os.system(cmd_str)
		
	sql_file = open(tmp_filename, 'r')
	sql_query = ""
	for line in sql_file:
		sql_query += line + " " 
		
	return sql_query
		
#
#
def row_comparator(rows1, rows2):

	# compare aql results with sql results
	for r1 in rows1:
		
		find = False
		for r2 in rows2:
		
			if len(r1) != len(r2):
				return False
		
			match = True
			for i in range(len(r1)):
				if str(r1[i]) != str(r2[i]):
					match = False
			
			if match:
				find = True
				break
		
		if not find:
			return False
		
	return True
