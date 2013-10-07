import random
from ExecuteSQL import ExecuteSQL

# ------------------------------------------------------------------------------
def __generate_table__(r_min, r_max, mode):
	l = r_max - r_min
	if mode == 'random':
		(t1_min, t1_max) = (r_min, r_max)
		(t2_min, t2_max) = (r_min, r_max)
	else:
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

		yield [ t1 , t2 ]
				
		if mode == 'random':
			break
				
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
	if len(rows) != len(tables):
		raise Exception("bad database")
	for i in range(len(tables)):
		print tables[i], rows[i]

# ------------------------------------------------------------------------------
def generate(nb_tables, nb_rows, r_min, r_max, all_values, mode):

	if nb_rows == -1:
		yield (None, None)
		return

	for tables_values in __generate_table__(r_min, r_max, mode):
		
		for i in range(len(tables_values) + 1, nb_tables + 1):
			# print 'use random values between', r_min, 'and', r_max, 'for table', 't' + str(i)
			if mode == 'random':
				tables_values.append(range(r_min, r_max + 1))
			else:
				tables_values.append([v for v in range(r_min - 10, r_max + 10)]) # FIXME
		
		# print 'Generate database with values:'
		# id = 1
		# for t in tables_values:
		# 	print 't' + str(id) + ' : ', t
		#  	id += 1

		rows = []
		for t in range(nb_tables):
			rows.append([])

		if all_values:
			rows = []
			for t in tables_values:
				rows.append([ v for v in t ])
		elif mode != 'random': # add min and max
			rows = []
			for t in tables_values:
				rows.append([min(t), max(t)])
				
		for j in range(len(tables_values)):
			for i in range(nb_rows - len(rows[j])):
				rows[j].append( tables_values[j][random.randint(0, len(tables_values[j]) - 1)] )

		yield ([ 't' + str(id) for id in range(1, nb_tables + 1) ], rows)
		
# ------------------------------------------------------------------------------
def load(con, tables, rows):

	# check
	if len(rows) != len(tables):
		raise Exception("error generating database")

	# create tables
	for table in tables:
		con.execute_and_commit('drop table if exists ' + table + ';')
		con.execute_and_commit('create table ' + table + '(id int not null, val_1 int not null) engine=innodb ;')
	
	# generate queries
	queries = []
	for i in range(len(rows)):
		id = 1
		queries.append('insert into ' + tables[i] + ' (id, val_1) values (' + str(id) + ', ' + str(rows[i][0]) + ')')
		id += 1
		for j in range(1, len(rows[i])):
			queries[i] += ', (' + str(id) + ', ' + str(rows[i][j]) + ')'
			id += 1

	# insert values
	for query in queries:
		# print query
		con.execute_and_commit(query + ';')

# ------------------------------------------------------------------------------
class DBGen:
	def __init__(self, nb_tables, nb_rows, min_value, max_value, all_values, mode, exec_sql):
		self._nb_tables = nb_tables
		self._nb_rows = nb_rows
		self._min_value = min_value
		self._max_value = max_value
		self._all_values = all_values
		self._mode = mode
		self._exec_sql = exec_sql

	def iterate(self):
		if self._nb_rows == -1:
			yield([], [])
		else:
			for (tables, rows) in generate(self._nb_tables, self._nb_rows, self._min_value, self._max_value, self._all_values, self._mode):
				load(self._exec_sql, tables, rows)
				yield (tables, rows)

# ------------------------------------------------------------------------------
if __name__ == '__main__':
	exec_sql = ExecuteSQL('localhost', 'tma', 'AlgoQuest', 'algoquest')
	db_gen = DBGen(4, 10, 1, 20, False, 'random', exec_sql)
	for tables, rows in db_gen.iterate():
		print tables
		for r in rows:
			print r