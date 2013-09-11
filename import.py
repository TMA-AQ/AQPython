#! /cygdrive/c/Python27/python

import os
import sys
import MySQLdb as mdb
import struct
import collections
from ctypes import create_string_buffer
from array import array

import util
from BaseDesc import BaseDesc, TableDesc, ColumnDesc

class ImportError(Exception):
	def __init__(self, message):
		self.message = message
		
#
# create directories
def create_db_directories(db_path, db_name):
	dir = db_path + '/' + db_name + '/'
	if os.path.exists(dir):
		raise ImportError('directory ' + dir + ' already exist')

	d = os.path.dirname(dir + 'base_struct/')
	os.makedirs(d)
	d = os.path.dirname(dir + 'calculus/')
	os.makedirs(d)
	d = os.path.dirname(dir + 'data_orga/')
	os.makedirs(d)
	d = os.path.dirname(dir + 'data_orga/columns/')
	os.makedirs(d)
	d = os.path.dirname(dir + 'data_orga/tables/')
	os.makedirs(d)
	d = os.path.dirname(dir + 'data_orga/tmp/')
	os.makedirs(d)
	d = os.path.dirname(dir + 'data_orga/vdg/')
	os.makedirs(d)
	d = os.path.dirname(dir + 'data_orga/vdg/data/')
	os.makedirs(d)
	
#
# generate ini
def generate_ini(db_path, db_name):
	ini_filename = db_path + '/' + db_name + '/' + db_name + '.ini'
	fini = open(ini_filename, 'w')
	fini.write('root-folder=' + db_path + '/' + db_name + '/' + '\n')
	fini.write('tmp-folder=' + db_path + '/' + db_name + '/data_orga/tmp/' + '\n')
	fini.write('field-separator=;' + '\n')
	fini.write('csv-format=1' + '\n')
	fini.write('aq-engine=E:/AQ_Bin/AQ_Engine.exe' + '\n')
	fini.write('aq-loader=E:/AQ_Bin/Loader.exe' + '\n')
	return ini_filename
	
#
# retrieve data from original database
def generate_base_desc(con, base_desc_filename):
	cur = con.cursor()
	
	cur.execute('show tables ;')
	tables = cur.fetchall()

	fdesc = open(base_desc_filename, 'w')
	fdesc.write(db_name)
	fdesc.write('\n')
	fdesc.write(str(cur.rowcount))
	fdesc.write('\n')
	fdesc.write('\n')
	
	table_id = 1
	for (table_name, ) in cur:
	
		cur.execute('select * from ' + table_name + ' limit 1')
		desc = cur.description
		
		fdesc.write('"' + table_name + '"')
		fdesc.write(' ')
		fdesc.write(str(table_id))
		fdesc.write(' ')
		fdesc.write(str(cur.rowcount))
		fdesc.write(' ')
		fdesc.write(str(len(desc)))
		fdesc.write('\n')
		table_id +=  1
		
		column_id = 1
		for c in desc:
			fdesc.write('"' + c[0] + '"')
			fdesc.write(' ')
			fdesc.write(str(column_id))
			fdesc.write(' 1 INT\n')
			column_id += 1
		fdesc.write('\n')
	
#
# 
def export_data(con, dir):
	cur = con.cursor()
	cur.execute('show tables ;')
	tables = cur.fetchall()

	for (table_name, ) in cur:
		f = open(dir + table_name + '.txt', 'w')
		cur.execute('select * from  ' + table_name)
		desc = cur.description
		for i in range(cur.rowcount):
			row = cur.fetchone()
			row_str = ""
			for i in range(len(row)):
				row_str += "\"" + str(row[i]) + "\""
				if (i + 1) < len(row):
					row_str += ","
			f.write(row_str + '\n')
		f.close()
	
#
#
def get_base_data_filename(base, table, column, version, packet):
	return 'B%03dT%04dC%04dV%02dP%013d' % (base, table, column, version, packet)
	
#
#
def write_thesaurus_value(the, value, type, size):
	if type == 'INT':
		bufferVar = create_string_buffer(4)
		struct.pack_into("!l", bufferVar, 0, int(value))
		the.write(bufferVar.raw)
	elif type == 'LONG':
		raise Exception('type ' + type + ' not supported')
	elif type == 'FLOAT':
		raise Exception('type ' + type + ' not supported')
	elif type == 'DOUBLE':
		raise Exception('type ' + type + ' not supported')
	elif type == 'CHAR':
		raise Exception('type ' + type + ' not supported')
	elif type == 'RAW':
		raise Exception('type ' + type + ' not supported')
	
#
# call AQTools loader
def import_data(db_ini_filename, packet_size = 1048576):
	
	# os.system('AQTools --aq-ini ' + db_ini_filename + ' --load-db' )
	
	f = open(db_ini_filename, 'r')
	db_path = ""
	for line in f:
		if line.find('root-folder') != -1:
			key, db_path = line.split('=')
			db_path = db_path.replace('\n', '')
			
	if db_path == "":
		raise ImportError('cannot find \'root-folder\' in ini file \'' + db_ini_filename + '\'')
	
	base_desc_filename = db_path + '/base_struct/base'
	db_desc = BaseDesc()
	db_desc.load_base_desc(base_desc_filename)
	
	table_export_path = db_path + '/data_orga/tables/'
	for name in os.listdir(table_export_path):
		table_name, ext = name.split('.')
		table = db_desc.GetTable(table_name)
		
		thes = dict()
		for c in table.columns:
			thes[c.id] = collections.OrderedDict()
		
		name = table_export_path + name
		if os.path.isfile(name):
			f = open(name, 'r')
			n = 1
			p = 0
			for line in f:
				row = line.split(',')
				if len(row) != len(table.columns):
					raise Exception('bad data file: rows doesn\'t match with column')
				for i in range(len(row)):
					value = row[i].strip(' \n"')
					cid = i + 1
					if thes[cid].get(value) == None:
						thes[cid][value] = []
					thes[cid][value] += [n]
				n += 1

		#
		#
		b, p, v, = 1, 0, 1
		for c, the in thes.iteritems():
		
			column = table.GetColumn(c)
			the_filename  = db_path + '/data_orga/vdg/data/' + get_base_data_filename(b, table.id, c, v, p) + '.the'
			prm_filename  = db_path + '/data_orga/vdg/data/'  + get_base_data_filename(b, table.id, c, v, p) + '.prm'
			
			fthe = open(the_filename, 'w')
			prm_data = []
			for i in range(n - 1):
				prm_data.append(0)
			i = 0
			for value, prm in the.iteritems():
				write_thesaurus_value(fthe, value, column.type, column.size)
				for p in prm:
					prm_data[p - 1] = i
				i += 1
			fthe.close()
			
			fprm = open(prm_filename, 'w')			
			a = array('L', prm)
			a.tofile(fprm)
			fprm.close()
				
#
#
if __name__ == '__main__':

	db_path, db_name = 'E:/AQ_DATABASES/DB/', 'test_group_by'
	db_host, db_user, db_pass, db_name = 'localhost', 'tma', 'AlgoQuest', 'test_group_by'
	
	try:
	
		con = mdb.connect(db_host, db_user, db_pass, db_name)
		
		create_db_directories(db_path, db_name)
		db_ini_filename = generate_ini(db_path, db_name)
	
		generate_base_desc(con, db_path + '/' + db_name + '/base_struct/base')
		export_data(con, db_path + '/' + db_name + '/data_orga/tables/')
		
		import_data(db_ini_filename)
		import_data(db_path + db_name + '/test_group_by.ini')
		
	except ImportError, e:
		print "Error: %s" % (e.message)
	except mdb.Error, e:
		print "Error: %d: %s" % (e.args[0], e.args[1])
	finally:
		if con:
			con.close()
