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
def row_in(rows1, rows2, ordered=False):

	if len(rows1) != len(rows2):
		return False

	for i1 in range(len(rows1)):
		
		if len(rows1[i1]) != len(rows2[i1]):
			return False
				
		# check rows2[i1] first		
		match = True	
		for k in range(len(rows1[i1])):
			vr1 = str(rows1[i1][k])
			vr2 = str(rows2[i1][k])
			if vr1 == 'None':
				vr1 = 'NULL'
			if vr2 == 'None':
				vr2 = 'NULL'
			if vr1 != vr2:
				match = False
				break
		if match:
			return True
			
		if ordered:
			return False
			
		# if not rows1[i1] and rows2[i1] are not equal and ordered is not important then check all rows[i1] on all rows2
		find = False
		for i2 in range(len(rows2)):

			match = True
			for k in range(len(rows1[i1])):
				vr1 = str(rows1[i1][k])
				vr2 = str(rows2[i2][k])
				if vr1 == 'None':
					vr1 = 'NULL'
				if vr2 == 'None':
					vr2 = 'NULL'
				if vr1 != vr2:
					match = False
					break

			if match:
				find = True
				break
		
		if not find:
			return False
		
	return True
