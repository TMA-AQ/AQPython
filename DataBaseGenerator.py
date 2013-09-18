import random
from ExecuteSQL import ExecuteSQL

def __generate_table__(r_min, r_max):
	l = r_max - r_min + 1
	(t1_min, t1_max) = (r_min, l/2 - 1)
	(t2_min, t2_max) = (l/2 + 1 + 1, r_max)
	(t1, t2) = ([], [])
	
	end = False
	while True:
		
		for i in range(t1_min, t1_max + 1):
			t1.append(i)
		
		for i in range(t2_min, t2_max + 1):
			t2.append(i)

		yield (t1, t2)
		
		if end:
			break
		
		(t1, t2) = ([], [])
		if t1_max < t2_min:
			t1_max, t2_min = t2_min, t1_max
		elif t1_min < t2_min:
			t1_min, t2_min = t2_min, t1_min
		elif t1_max < t2_max:
			t1_max, t2_max = t2_max, t1_max
		elif t1_min < t2_max:
			t1_min, t2_max = t2_max, t1_min			
		else:
			t1_min, t1_max, t2_min, t2_max = t2_min, t1_max, t2_max, t1_min
			end = True
		
def generate(n_tuple, r_min, r_max):

	for (t1, t2) in __generate_table__(r_min, r_max):
		
		print 'Generate database with values:'
		print 't1 : ', t1, '\nt2 : ', t2
		print ''
		
		tables = ['t1', 't2']
		tuples = []
		for i in range(n_tuple):
			tuples.append( [ t1[random.randint(0, len(t1) - 1)], t2[random.randint(0, len(t2) - 1)] ] )
			
		# print 't1 t2'
		# for t in tuples:
		# 	print t
		
		yield (tables, tuples)
		
def load(con, tables, tuples):

	# create tables
	for table in tables:
		con.execute_and_commit('drop table if exists ' + table + ';')
		con.execute_and_commit('create table ' + table + '(id int not null) engine=innodb ;')
	
	# generate queries
	queries = []
	for tuple in tuples:
		if len(tuple) != len(tables):
			print >> sys.stderr, "error generating database: bad size for tuple ", tuple
		for i in range(len(tuple)):
			if i >= len(queries):
				queries.append('insert into ' + tables[i] + ' values (' + str(tuple[i]) + ')')
			else:
				queries[i] += ', (' + str(tuple[i]) + ')'
				
	# insert values
	for query in queries:
		con.execute_and_commit(query + ';')
