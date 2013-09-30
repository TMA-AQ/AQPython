import sys
import xml.dom.minidom as xml

# ------------------------------------------------------------------------------
def getText(nodelist):
	rc = []
	for node in nodelist:
		if node.nodeType == node.TEXT_NODE:
			rc.append(node.data)
	return ''.join(rc).strip()

# ------------------------------------------------------------------------------
def getList(valueslist):
	valueslist = valueslist.strip(' []\n')
	return [ v.strip() for v in valueslist.split(',') ]

# ------------------------------------------------------------------------------
def print_db_errors(test):

	print 'Test', test.attributes["id"].value, ':'
	print ''

	#
	for db in test.getElementsByTagName("Database"):
		for table in db.getElementsByTagName("Table"):
			tname = table.attributes["name"].value
			qc = 'drop table if exists ' + tname + ' ; create table ' + tname + ' ( '
			qi = 'insert into ' + tname + ' ( '
			cnames = []
			values = []
			for column in table.getElementsByTagName("Column"):
				cnames.append(column.attributes["name"].value)
				values.append(getList(getText(column.childNodes)))

			qc += ' int , '.join(cnames) + ' int ) engine=innodb ;'
			qi += ', '.join(cnames) + ' ) values '
			for i in range(len(values[0])):
				qi += '('
				for j in range(len(values)):
					qi += values[j][i]
					if j < (len(values) - 1):
						qi += ', '
				qi += ')'
				if i < (len(values[0]) - 1):
					qi += ', '
			qi += ' ;'
			
			print qc
			print qi

	print ''
	
	#
	for query in test.getElementsByTagName("Query"):
		print '---'
		print ''
		for aql_query in query.getElementsByTagName("AQL"):
			print getText(aql_query.childNodes)
		print ''
		for sql_query in query.getElementsByTagName("SQL"):
			print getText(sql_query.childNodes)
		print ''
		for result in query.getElementsByTagName("Results"):
			print 'results :', getText(result.childNodes)
		print ''
		for expected in query.getElementsByTagName("Expected"):
			print 'expected :', getText(expected.childNodes)
		print ''

# ------------------------------------------------------------------------------
class DBGen:
	def __init__(self, exec_sql, xml_file_name, id = None):
		self._exec_sql = exec_sql
		self._tests = xml.parse(xml_file_name)
		self._id = id

	def iterate(self, ):
		for test in self._tests.getElementsByTagName("Test"):

			#
			if self._id is not None and test.attributes["id"].value != id:
				continue
	
			print 'Test', test.attributes["id"].value
			for db in test.getElementsByTagName("Database"):
				tnames = []
				rows = []
				for table in db.getElementsByTagName("Table"):
					tname = table.attributes["name"].value
					tnames.append(tname)
					q_drop = 'drop table if exists ' + tname + ' ; '
					q_create = 'create table ' + tname + ' ( '
					q_insert = 'insert into ' + tname + ' ( '
					cnames = []
					values = []
					for column in table.getElementsByTagName("Column"):
						cnames.append(column.attributes["name"].value)
						values.append(getList(getText(column.childNodes)))
					self._generate_db(q_drop, q_create, q_insert, cnames, values)
				yield (tnames , rows)
	

	def _generate_db(self, qd, qc, qi, cnames, values):
		qc += ' int , '.join(cnames) + ' int ) engine=innodb ;'
		qi += ', '.join(cnames) + ' ) values '
		for i in range(len(values[0])):
			qi += '('
			for j in range(len(values)):
				qi += values[j][i]
				if j < (len(values) - 1):
					qi += ', '
			qi += ')'
			if i < (len(values[0]) - 1):
				qi += ', '
		qi += ' ;'
				
		print qd
		print qc
		print qi
		
		self._exec_sql.execute_and_commit(qd)
		self._exec_sql.execute_and_commit(qc)
		self._exec_sql.execute_and_commit(qi)


# ------------------------------------------------------------------------------
if __name__ == '__main__':
	file = sys.argv[1]
	if len(sys.argv) > 2:
		id = sys.argv[2]
	
	dom1 = xml.parse(file)
	
	for test in dom1.getElementsByTagName("Test"):
	
		#
		if id is not None and test.attributes["id"].value == id:
			print_db_errors(test)
			break
		else:
			print '===================================='
			print ''
			print_db_errors(test)
			print ''
