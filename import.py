#! /usr/bin/python

import os
import sys
import MySQLdb as mdb
import collections
from optparse import OptionParser, OptionGroup

import util
import loader
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
def generate_ini(db_path, db_name, aq_engine, aq_loader):
	ini_filename = db_path + '/' + db_name + '/' + db_name + '.ini'
	fini = open(ini_filename, 'w')
	fini.write('root-folder=' + db_path + '/' + db_name + '/' + '\n')
	fini.write('tmp-folder=' + db_path + '/' + db_name + '/data_orga/tmp/' + '\n')
	fini.write('field-separator=;' + '\n')
	fini.write('csv-format=1' + '\n')
	fini.write('aq-engine=' + aq_engine + '\n')
	fini.write('aq-loader=' + aq_loader + '\n')
	fini.close()
	return ini_filename
	
#
# retrieve data from original database
def generate_base_desc(con, db_name, base_desc_filename):
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

	fdesc.close()
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
if __name__ == '__main__':

	parser = OptionParser()
	
	#
	# algoquest options
	aq_options = OptionGroup(parser, "AlgoQuest options")
	aq_options.add_option('', '--aq-db-name', 
												action="store", 
												type="string", 
												dest="aq_db_name", 
												default="algoquest", 
												help='algoquest database name [default: %default]')
	aq_options.add_option('', '--aq-db-path', 
												action="store", 
												type="string", 
												dest="aq_db_path", 
												help='mandatory algoquest database path')
	aq_options.add_option('', '--aq-engine', 
												action="store", 
												type="string", 
												dest="aq_engine", 
												default="AQ_Engine", 
												help='aq engine executable [default: %default]')
	aq_options.add_option('', '--aq-loader', 
												action="store", 
												type="string", 
												dest="aq_loader", 
												default="AQ_Loader", 
												help='loader executable [default: %default]')				
										
	#
	# source database
	source_db_options = OptionGroup(parser, "Source database options")
	source_db_options.add_option('', '--db-host', 
															action="store", 
															type="string", 
															dest="db_host", 
															default="localhost", 
															help='source database host [default: %default]')
	source_db_options.add_option('', '--db-user', 
															action="store", 
															type="string", 
															dest="db_user", 
															default="algoquest", 
															help='source database user [default: %default]')
	source_db_options.add_option('', '--db-pass', 
															action="store", 
															type="string", 
															dest="db_pass", 
															default="algoquest", 
															help='source database password [default: %default]')
	source_db_options.add_option('', '--db-name', 
															action="store", 
															type="string", 
															dest="db_name", 
															default="algoquest", 
															help='source database name [default: %default]')
										
	parser.add_option_group(aq_options)
	parser.add_option_group(source_db_options)

	opts, args = parser.parse_args()
	
	if opts.aq_db_path is None:
		print "A Mandatory option is missing\n"
		parser.print_help()
		sys.exit(-1)
	
	try:
	
		con = mdb.connect(opts.db_host, opts.db_user, opts.db_pass, opts.db_name)
		
		create_db_directories(opts.aq_db_path, opts.aq_db_name)
		db_ini_filename = generate_ini(opts.aq_db_path, opts.aq_db_name, opts.aq_engine, opts.aq_loader)
	
		generate_base_desc(con, opts.aq_db_name, opts.aq_db_path + '/' + opts.aq_db_name + '/base_struct/base')
		export_data(con, opts.aq_db_path + '/' + opts.aq_db_name + '/data_orga/tables/')
		
		# import_data(db_ini_filename)
		loader.load_data(db_ini_filename)
		
	except ImportError, e:
		print "Error: %s" % (e.message)
	except mdb.Error, e:
		print "Error: %d: %s" % (e.args[0], e.args[1])
	finally:
		if con:
			con.close()
