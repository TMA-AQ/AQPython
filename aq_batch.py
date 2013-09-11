#! /cygdrive/c/Python27/python

import os

from ExecuteAQL import ExecuteAQL

db_path = 'E:/AQ_DATABASES/DB/'
db_name = 'BNP'
ident = 'level_4'
working_dir = db_path + db_name + '/calculus/' + ident + '/'
tmp_working_dir = db_path + db_name + '/data_orga/tmp/' + ident + '/'
ini_filename = working_dir + 'aq_engine.ini'

if not os.path.exists(working_dir):
	d = os.path.dirname(working_dir)
	os.makedirs(d)

if not os.path.exists(tmp_working_dir):
	d = os.path.dirname(tmp_working_dir)
	os.makedirs(d)
else:
	print "clean " + tmp_working_dir
	
f = open(ini_filename, 'w')
f.write('export.filename.final=' + db_path + db_name + '/base_struct/base.\n')
f.write('step1.field.separator=;\n')
f.write('k_rep_racine=' + db_path + db_name + '/\n')
f.write('k_rep_racine_tmp=' + db_path + db_name + '/\n')
f.close()

exec_aql = ExecuteAQL(db_path, db_name)

query_level_6  = ' SELECT . tw_flux_quotidien_hebdo_diag seq_point_de_vente_histo \n'
query_level_6 += ' FROM tw_flux_quotidien_hebdo_diag \n'
query_level_6 += ' WHERE K_JNO . tw_flux_quotidien_hebdo_diag seq_point_de_vente_histo \n'
query_level_6 += ' GROUP , , , , , '
query_level_6 += ' . tw_flux_quotidien_hebdo_diag seq_point_de_vente_histo '
query_level_6 += ' . tw_flux_quotidien_hebdo_diag seq_cbp_histo '
query_level_6 += ' . tw_flux_quotidien_hebdo_diag seq_caf_histo '
query_level_6 += ' . tw_flux_quotidien_hebdo_diag seq_produit ' 
query_level_6 += ' . tw_flux_quotidien_hebdo_diag seq_indicateur '
query_level_6 += ' . tw_flux_quotidien_hebdo_diag seq_sem_court '

query_filename = db_path + db_name + '/calculus/' + ident + '/New_Request.txt'
f = open(query_filename, 'w')
f.write(query_level_6)
f.close()

exec_aql.run_aq_engine(ini_filename, ident, 'dpy')

query_level_5 = ''

query_level_4 = ''