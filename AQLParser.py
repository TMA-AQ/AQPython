#
#
def __op_to_str__(op):
	op = op.upper()
	if op == 'K_JEQ':
		return '='
	elif op == 'K_JINF':
		return '<'
	elif op == 'K_JIEQ':
		return '<='
	elif op == 'K_JSUP':
		return '>'
	elif op == 'K_JSEQ':
		return '>='
#
#
def __join_to_str__(j_left, j_right):
	j_left = j_left.upper()
	j_right = j_right.upper()
	if j_left == 'K_INNER' and j_right == 'K_INNER':
		return 'inner join'
	elif j_left == 'K_OUTER' and j_right == 'K_INNER':
		return 'left outer join'
	elif j_left == 'K_INNER' and j_right == 'K_OUTER':
		return 'right outer join'
	elif j_left == 'K_OUTER' and j_right == 'K_OUTER':
		return 'full outer join'

#
#
def __parse_columns_list__(columns):
	list = []
	n = 1
	for i in columns.split(' '):
		if i == ',':
			n += 1
	for i in columns.split('.')[1:]:
		list.append(i.strip(' ').split(' '))
	if n != len(list):
		raise Exception('wrong aql query')
	return list

#
#
class Statements:

	#
	#
	def __init__(self):
		self.selectStmt = []
		self.fromStmt = []
		self.joinStmt = []
		self.inStmt = []
		self.groupStmt = []
		self.orderStmt = []

	#
	#
	def __parse_select_stmt__(self, selectStmt):
		self.selectStmt = __parse_columns_list__(selectStmt[len('SELECT'):].strip(' '))
	
	#
	#
	def __parse_group_stmt__(self, selectStmt):
		self.groupStmt = __parse_columns_list__(selectStmt[len('GROUP'):].strip(' '))

	#
	#
	def __parse_order_stmt__(self, selectStmt):
		self.orderStmt = __parse_columns_list__(selectStmt[len('ORDER'):].strip(' '))
				
	#
	#
	def __parse_from_stmt__(self, fromStmt):
		fromStmt = fromStmt[len('FROM'):].strip(' ')
		n = 1
		for i in fromStmt.split(' '):
			if i == ',':
				n += 1
		for i in fromStmt.split(' ')[n-1:]:
			self.fromStmt.append(i.strip(' '))
		if n != len(self.fromStmt):
			raise Exception('wrong aql query')
		
	#
	#
	def __parse_where_stmt__(self, whereStmt):
		whereStmt = whereStmt[len('WHERE'):].strip(' ')
		n = 1
		for i in whereStmt.split(' '):
			if i.upper() == 'AND':
				n += 1
		conds = [ c.strip(' \n') for c in whereStmt.split(' ')[n-1:] ]
		i = 0
		for k in range(n):
			if (conds[i].upper() == 'K_JEQ' or 
					conds[i].upper() == 'K_JINF' or
					conds[i].upper() == 'K_JIEQ' or
					conds[i].upper() == 'K_JSUP' or
					conds[i].upper() == 'K_JSEQ' ):
				self.joinStmt.append(conds[i:i+9])
				i += 9
			elif conds[i].upper() ==	'K_JNO':
				i += 4
			elif conds[i].upper() == 'IN':
				try:
					j = i + conds[i+1:].index('IN') + 1
				except ValueError:
					j = len(conds)
				self.inStmt.append(conds[i:i+4] + [ v for v in conds[i+5:j] if v != ',' and v != 'K_VALUE' ])
				i = j
			else:
				raise Exception('wrong aql query')

	#
	#
	def parse(self, query):
		
		query = query.replace('\n', ' ')
		
		# 
		# select statement
		beg = query.upper().find('SELECT')
		end = query.upper().find('FROM')
		if (beg != -1) and (end != -1):
			self.__parse_select_stmt__(query[beg:end].strip(' '))
			
		# 
		# from statement
		beg = query.upper().find('FROM')
		end = query.upper().find('WHERE')
		if (beg != -1) and (end != -1):
			self.__parse_from_stmt__(query[beg:end].strip(' '))
		
		# 
		# where statement
		beg = query.upper().find('WHERE')
		end = query.upper().find('GROUP')
		if end == -1:
			end = query.upper().find('ORDER')
		if end == -1:
			end = query.upper().find(';')
		if (beg != -1) and (end != -1):
			self.__parse_where_stmt__(query[beg:end].strip(' '))
		
		# 
		# group statement
		beg = query.upper().find('GROUP')
		if beg != -1:
			end = query.upper().find('ORDER')
			if end == -1:
				end = query.upper().find(';')
			if (beg != -1) and (end != -1):
				self.__parse_group_stmt__(query[beg:end].strip(' '))
		
		# 
		# order statement
		beg = query.upper().find('ORDER')
		if beg != -1:
			end = query.upper().find(';')
			if (beg != -1) and (end != -1):
				self.__parse_order_stmt__(query[beg:end].strip(' '))
		
	#
	#
	def to_sql(self, separator=' '):
		sql_query = ''
		
		# select
		sql_query = 'SELECT '
		for i in range(len(self.selectStmt)):
			if i > 0:
				sql_query += ', '
			sql_query += self.selectStmt[i][0] + '.' + self.selectStmt[i][1]
		
		sql_query += separator
		
		# from
		sql_query += 'FROM '
		
		for i in range(len(self.joinStmt)):
			sql_query += separator + '  '
			if i == 0:
				sql_query += self.joinStmt[i][7]
			sql_query += ' ' + __join_to_str__(self.joinStmt[i][5], self.joinStmt[i][1]) + ' '
			sql_query += self.joinStmt[i][3]
			sql_query += ' on ('
			sql_query += self.joinStmt[i][7] + '.' + self.joinStmt[i][8]
			sql_query += ' ' + __op_to_str__(self.joinStmt[i][0]) + ' '
			sql_query += self.joinStmt[i][3] + '.' + self.joinStmt[i][4]
			sql_query += ')'		
		
		sql_query += separator
		
		# where
		if len(self.inStmt) > 0:
			sql_query += 'WHERE '
		for i in range(len(self.inStmt)):
			if i > 0:
				sql_query += ' and '
			sql_query += separator + '  '
			sql_query += self.inStmt[i][2] + '.' + self.inStmt[i][3] + ' in (' 
			for j in range(4, len(self.inStmt[i])):
				if j > 4:
					sql_query += ', '
				sql_query += self.inStmt[i][j]
			sql_query += ')'
		
		# group by
		if len(self.groupStmt) > 0:
			sql_query += separator
			sql_query += 'GROUP BY '
			for i in range(len(self.groupStmt)):
				if i > 0:
					sql_query += ', '
				sql_query += self.groupStmt[i][0] + '.' + self.groupStmt[i][1]
		
		# order by
		if len(self.orderStmt) > 0:
			sql_query += separator
			sql_query += 'ORDER BY '
			for i in range(len(self.orderStmt)):
				if i > 0:
					sql_query += ', '
				sql_query += self.orderStmt[i][0] + '.' + self.orderStmt[i][1]
		
		sql_query += separator
		sql_query += ';'
		
		return sql_query
