import random
from ExecuteSQL import ExecuteSQL

# ------------------------------------------------------------------------------
def __generate_table__(r_min, r_max):
	l = r_max - r_min
	(t1_min, t1_max) = (r_min, r_min + l/4)
	(t2_min, t2_max) = (r_min + l/2, r_max)
	(t1, t2) = ([], [])
	
	end = False
	while not end:
		
		for i in range(t1_min, t1_max + 1):
			t1.append(i)
		
		for i in range(t2_min, t2_max + 1):
			t2.append(i)
		
		# print '(t1 , t2) : ( [', t1_min, t1_max, '] , [', t2_min, t2_max, '] )'

		yield (t1, t2)
				
		(t1, t2) = ([], [])

		if t1_min < t2_min:
			# 5 cases of t1_max
			if t1_max < t2_min:
				t1_max = t2_min
			elif t1_max == t2_min:
				t1_max = (t2_min + t2_max) / 2
			elif t1_max > t2_min and t1_max < t2_max:
				t1_max = t2_max
			elif t1_max == t2_max:
				t1_max = t2_max + (l / 2)
			else:
				t1_min = t2_min
				t1_max = (t2_min + t2_max) / 2
		elif t1_min == t2_min:
			# 3 cases of t1_max
			if t1_max < t2_max:
				t1_max = t2_max
			elif t1_max == t2_max:
				t1_max = t2_max + (l / 2)
			else:
				t1_min = (t2_min + t2_max) / 2
				t1_max = (t2_min + t2_max) / 2
		elif t1_min > t2_min and t1_min < t2_max:
			# 3 cases of t1_max
			if t1_max < t2_max:
				t1_max = t2_max
			elif t1_max == t2_max:
				t1_max = t2_max + (l / 2)
			else:
				t1_min = t2_max
				t1_max = t2_max + (l / 2)
		elif t1_min == t2_max:
			# 1 case of t1_max
			t1_min = t2_max + (l / 3)
		elif t1_min > t2_max:
			# 1 case of t1_max
			end = True
						
# ------------------------------------------------------------------------------
def print_base(tables, rows):
	for i in range(len(tables)):
		print tables[i], [r[i] for r in rows]

# ------------------------------------------------------------------------------
def generate(nb_rows, r_min, r_max):

	if nb_rows == -1:
		yield (None, None)
		return

	for (t1, t2) in __generate_table__(r_min, r_max):
		
		# print 'Generate database with values:'
		# print 't1 : ', t1, '\nt2 : ', t2
		# print ''
		
		tables = ['t1', 't2']
		rows = []
		for i in range(nb_rows):
			rows.append( [ t1[random.randint(0, len(t1) - 1)], t2[random.randint(0, len(t2) - 1)] ] )
				
		yield (tables, rows)
		
# ------------------------------------------------------------------------------
def load(con, tables, rows):

	# create tables
	for table in tables:
		con.execute_and_commit('drop table if exists ' + table + ';')
		con.execute_and_commit('create table ' + table + '(id int not null, val_1 int not null) engine=innodb ;')
	
	# generate queries
	id = 1
	queries = []
	for row in rows:
		if len(row) != len(tables):
			print >> sys.stderr, "error generating database: bad size for row ", row
		for i in range(len(row)):
			if i >= len(queries):
				queries.append('insert into ' + tables[i] + ' values (' + str(id) + ', ' + str(row[i]) + ')')
			else:
				queries[i] += ', (' + str(id) + ', ' + str(row[i]) + ')'
		id += 1

	# insert values
	for query in queries:
		con.execute_and_commit(query + ';')
