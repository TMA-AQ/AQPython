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
