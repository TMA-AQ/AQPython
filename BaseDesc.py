class BaseDesc:
	def __init__(self):
		self.name = ""
		self.tables = []
	
	def __str__(self):
		desc = self.name + '\n\n'
		for t in self.tables:
			desc += str(t) + '\n'
		return desc
		
	def load_base_desc(self, filename):
		f = open(filename, 'r')
		
		self.name = f.readline().strip('\n')
		nb_table = f.readline().strip('\n')
		f.readline()
		
		for t in range(int(nb_table)):
			table = TableDesc()
			table.name, id, nb_row, nb_col = f.readline().strip('\n').split(' ')
			table.name = table.name.strip('"')
			table.id = int(id)
			for c in range(int(nb_col)):
				column = ColumnDesc()
				column.name, id, size, column.type = f.readline().strip('\n').split(' ')
				column.name = column.name.strip('"')
				column.id = int(id)
				column.size = int(size)
				column.type
				table.columns.append(column)
			
			self.tables.append(table)
			f.readline()
		
		f.close()
		
	def GetTable(self, table_name):
		for t in self.tables:
			if t.name == table_name:
				return t
		raise Exception('cannot find table ' + table_name)
	
class TableDesc:
	def __init__(self):
		self.id = -1
		self.name = ""
		self.columns = []
	
	def __str__(self):
		desc = self.name + ' ' + str(self.id) + '\n'
		for c in self.columns:
			desc += str(c) + '\n'
		return desc
	
	def GetColumn(self, column_id):
		for c in self.columns:
			if c.id == column_id:
				return c
		raise Exception('cannot find column ' + column_id)
	
class ColumnDesc:
	def __init__(self):
		self.id = -1
		self.name = ""
		self.type = "INT"
		self.size = 1
	
	def __str__(self):
		return self.name + ' ' + str(self.id) + ' ' + self.type + ' ' + str(self.size)