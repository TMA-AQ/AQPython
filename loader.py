import os
import struct
from ctypes import create_string_buffer
from array import array

from BaseDesc import BaseDesc

#
#
def __write_thesaurus_value__(the, value, type, size):
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

def __generate_loader_ini__(db_path):
	ini_filename = db_path + '/loader.ini'
	f = open(db_path + '/loader.ini', 'w')
	f.write('export.filename.final=' + db_path + '/base_struct/base.' + '\n')
	f.write('step1.field.separator=,' + '\n')
	f.write('k_rep_racine=' + db_path + '\n')
	f.write('k_rep_racine_tmp=' + db_path + '\n')
	f.write('k_taille_nom_fichier=1024' + '\n')
	f.close()
	return ini_filename
		
#
# call external Loader
def load_data(db_ini_filename):
	os.system('AQTools --aq-ini ' + db_ini_filename + ' --load-db' )
	
	# f = open(db_ini_filename)
	# for line in f:
	# 	key, value = line.split('=')
	# 	if key == 'aq-loader':
	# 		loader = value.strip('\n')
	# 	elif key == 'root-folder':
	# 		db_path = value.strip('\n')
	# 
	# print loader
	# print db_path
	# 
	# if loader is None or db_path is None:
	# 	raise Exception('missing key in ini file')
	# 
	# ini_filename = __generate_loader_ini__(db_path)
	# 
	# bd = BaseDesc()
	# bd.load_base_desc(db_path + 'base_struct/base')
	# print bd
	# 
	# for t in bd.tables:
	# 	for c in t.columns:
	# 		cmd = loader + ' ' + ini_filename + ' ' + str(t.id) + ' ' + str(c.id)
	# 		print cmd
	# 		os.system(cmd)
	
#
# import thesaurus and prm data files
def import_data(db_ini_filename, packet_size = 1048576):
	
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
				