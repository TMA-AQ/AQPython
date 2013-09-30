import sys
import util
import re

class QueryGenerator:

	#
	#
	def __init__(self, base, idents, ops):
		self.base = base
		self.idents = idents
		self.ops = ops
		self._values = []
		
		for ident in self.idents:
			for op_name, values in ops.iteritems():
				if ident.find(op_name) != -1:
					if ident[0:5] == '[MIX_':
						self._values.append([ident] + [' ,' * (len(p)-1) + ' ' + ' '.join(p) for p in util.permut(values)])
					else:	
						self._values.append([ident] + values)
					
	#
	#
	def next(self):
		
		if len(self._values[0]) == 1:
			return ""
			
		aql_query = self.base
		unpack = True

		for i in reversed(range(len(self._values))):
			
			aql_query = aql_query.replace(self._values[i][0], self._values[i][-1])
			
			if unpack and ((i == 0) or (len(self._values[i]) > 2)): # ident + first value
					
					# remove last item
					self._values[i].pop()
					
					# reinitialize other
					for j in range(i + 1, len(self._values)):
						self._values[j][1:] = []
						for op_name, values in self.ops.iteritems():
							if self._values[j][0].find(op_name) != -1:
								self._values[j] += values
					
					unpack = False
		
		return aql_query
		
#
#
def parse(f):

	base = ""
	ops = dict()
	for line in f:
		if line.find('=') != -1:
			op_name, values_str = line.split('=');
			op_name = op_name.strip()
			values_str = values_str.strip(' []\n')
			values = values_str.split(',')
			for i in range(len(values)):
				values[i] = values[i].strip(' \'')
			ops[op_name] = values
		elif line.strip() != "":
			base += line

	idents = []
	for op_name in ops:
		idents += re.findall('\[' + op_name + '_[0-9]+\]', base)

	if ';' in base:
		gen = QueryGenerator(base, idents, ops)
		return gen
	else:
		return None

#
#
def iterate(input):
	
	name, ext = input.split('.')

	# 
	# 
	if ext == 'aql':
		f = open(input)
		query = ''
		for line in f:
			if line.strip() != '':
				query += line
			if ';' in line:
				yield query
				query = ''

	#
	#
	elif ext == 'gen':
		f = open(input, 'r')
		while True:
			gen = parse(f)
			if gen is None:
				break
			find = True
			while find:
				query = gen.next()
				if query == "":
					find = False
				else:
					yield query
	
	#
	#
	else:
		raise Exception(input + ' is a non valid filename (only .aql and .gen are accepted)')
	
#
#
if __name__ == '__main__':

	for query in iterate(sys.argv[1]):
		print query
