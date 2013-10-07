#! /usr/bin/python

import os, sys, getopt, time, math
from optparse import OptionParser, OptionGroup
import ConfigParser

from ExecuteAQL import ExecuteAQL
from ExecuteSQL import ExecuteSQL
import AQLParser
import util
import QueryGenerator
import DataBaseGenerator
# import LogAnalyzer
import AQImport

# -------------------------------------------------------------------------------
def print_query(out, msg, aql_query, sql_query, aql_rows, sql_rows):
	print >> sys.stderr, ''
	print >> sys.stderr, '==========='
	print >> sys.stderr, '==========='
	print >> sys.stderr, ''
	print >> sys.stderr, msg
	print >> sys.stderr, ''
	print >> sys.stderr, '=== AQL ==='
	print >> sys.stderr, aql_query
	print >> sys.stderr, '--- result ---'
	print >> sys.stderr, aql_rows
	print >> sys.stderr, ''
	print >> sys.stderr, '=== SQL ==='
	print >> sys.stderr, sql_query.strip(' \n')
	print >> sys.stderr, '--- result ---'
	print >> sys.stderr, sql_rows
	print >> sys.stderr, ''
	print >> sys.stderr, '==========='
	print >> sys.stderr, '==========='
	print >> sys.stderr, ''

# -------------------------------------------------------------------------------
def parse_option(cfg):
	parser = OptionParser()

	#
	# algoquest database options
	section = 'AlgoQuest Options'
	aq_options = OptionGroup(parser, section)
	aq_options.add_option('', '--aq-db-name', action="store", type="string", dest="aq_db_name", default=cfg.get(section, 'aq-db-name'), help='algoquest database name [default: %default]')
	aq_options.add_option('', '--aq-db-path', action="store", type="string", dest="aq_db_path", default=cfg.get(section, 'aq-db-path'), help='algoquest database path [default: %default]')
	aq_options.add_option('', '--aq-engine', action="store", type="string", dest="aq_engine", default=cfg.get(section, 'aq-engine'), help='aq engine executable [default: %default]')
	aq_options.add_option('', '--aq-loader', action="store", type="string", dest="aq_loader", default=cfg.get(section, 'aq-loader'), help='loader executable [default: %default]')				
	aq_options.add_option('', '--aq-tools', action="store", type="string", dest="aq_tools", default=cfg.get(section, 'aq-tools'), help='aq tools executable [default: %default]')
	aq_options.add_option('', '--aq-engine-tests', action="store", type="string", dest="aq_engine_tests", default=cfg.get(section, 'aq-engine-tests'), help='aq engine tests executable [default: %default]')				
							
	#
	# reference database options (mysql)		
	section = 'Source Database Options'
	source_db_options = OptionGroup(parser, section)
	source_db_options.add_option('', '--import-db', action="store_true", dest="import_db", default=cfg.getboolean(section, 'import-db'), help="import database [default: %default]")
	source_db_options.add_option('', '--db-host', action="store", type="string", dest="db_host", default=cfg.get(section, 'db-host'), help='source database host [default: %default]')
	source_db_options.add_option('', '--db-user', action="store", type="string", dest="db_user", default=cfg.get(section, 'db-user'), help='source database user [default: %default]')
	source_db_options.add_option('', '--db-pass', action="store", type="string", dest="db_pass", default=cfg.get(section, 'db-pass'), help='source database password [default: %default]')
	source_db_options.add_option('', '--db-name', action="store", type="string", dest="db_name", default=cfg.get(section, 'db-name'), help='source database name [default: %default]')

	# 
	# check options
	section = 'Check Options'
	check_options = OptionGroup(parser, section)
	check_options.add_option('-c', '--check-db', action="store_true", dest="check_db", default=cfg.getboolean(section, 'check-db'), help="perform check comparasion between algoquest database and source database [default: %default]")
	check_options.add_option('-s', '--stop-on-failure', action="store_true", dest="stop_on_failure", default=cfg.getboolean(section, 'stop-on-failure'), help="stop testing when an error occur [default: %default]")
	check_options.add_option('-f', '--queries-file', action="store", type="string", dest="queries_file", default=cfg.get(section, 'queries-file'), help="queries file (generator(.gen) or list of aql queries(.aql)) [default: %default]")
							
	#
	# database generation options
	section = 'Database Generation Options'
	db_gen_options = OptionGroup(parser, section)
	db_gen_options.add_option('', '--generate-db', action="store_true", dest="generate_db", default=cfg.getboolean(section, 'generate-db'), help="generate database [default: %default]")
	db_gen_options.add_option('', '--generate-mode', action="store", type="string", dest="generate_mode", default=cfg.get(section, 'generate-mode'), help='generation mode[default: %default]')
	db_gen_options.add_option('', '--nb-tables', action="store", type="int", dest="nb_tables", default=cfg.get(section, 'nb-tables'), help="nb tables to generate [default: %default]")
	db_gen_options.add_option('', '--nb-rows', action="store", type="int", dest="nb_rows", default=cfg.get(section, 'nb-rows'), help="nb rows generate for each tables [default: %default]")
	db_gen_options.add_option('', '--min-value', action="store", type="int", dest="min_value", default=cfg.get(section, 'min-value'), help="minimal value generate in table t1 [default: %default]")
	db_gen_options.add_option('', '--max-value', action="store", type="int", dest="max_value", default=cfg.get(section, 'max-value'), help="maximal value generate in table t2 [default: %default]")
	db_gen_options.add_option('', '--all-values', action="store_true", dest="all_values", default=cfg.getboolean(section, 'all-values'), help="put all possible values in the generated database [default: %default]")

	#
	# miscellaneous option
	section = 'Miscellaneous Options'
	misc_options = OptionGroup(parser, section)
	misc_options.add_option('-v', '--verbose', action="store_true", dest="verbose", default=False, help="set verbosity")
	misc_options.add_option('-l', '--log-file', action="store", type="string", dest="log_file", help="activate log and log in LOG_FILE")
	misc_options.add_option('', '--xml-log-file', action="store", type="string", dest="xml_log_file", default=cfg.get(section, 'xml-log-file'), help="activate xml log and log in LOG_FILE [%default]")
	misc_options.add_option('', '--cfg', action="store", type="string", dest="cfg_file", default="", help="config filename, do not take care of other option if specified [default: aq_test.cfg]")

	parser.add_option_group(aq_options)
	parser.add_option_group(source_db_options)
	parser.add_option_group(check_options)
	parser.add_option_group(db_gen_options)
	parser.add_option_group(misc_options)
	
	return parser.parse_args()

# -------------------------------------------------------------------------------
def check_database(queries_file, exec_sql, exec_aql, stop_on_failure=False, verbose=False):

	queries_log = ''

	if queries_file is None:
		raise Exception('you need to provide a generator queries file')

	sum_rows = 0
	nb_checked, nb_error = 0, 0
	sql_sum_time, aql_sum_time = 0, 0
	for aql_query in QueryGenerator.iterate(queries_file):
		
		ss = AQLParser.Statements()
		ss.parse(aql_query)
		sql_query = ss.to_sql(separator = '\n')
		# aql_query = AQLParser.kjeq_parse(aql_query) # FIXME
			
		if verbose:
			print ''
			print '==='
			print ''		
			print aql_query
			print ''		
			print sql_query
					
		##################################################
		# skip full outer because not supported by mysql #
		if sql_query.lower().find('full outer') != -1:
			if verbose:
				print "full outer not supported, skip query"
			continue
		##################################################
			
		nb_checked += 1

		rc, sql_time, sql_rows = exec_sql.execute_and_fetch(sql_query)
		rc, aql_time, aql_rows = exec_aql.execute(aql_query)

		if (rc != 0) or (not util.row_in(sql_rows, aql_rows)) or (not util.row_in(aql_rows, sql_rows)):
			nb_error += 1
			if verbose:
				print_query(sys.stderr, 'ERROR: query failed', aql_query, sql_query, aql_rows, sql_rows)
			elif rc == 0:
				sys.stdout.write('d')
			elif rc != 0:
				sys.stdout.write('e')
			queries_log += '<Query id="' + str(nb_checked) + '" status="error" type="' + ('data' if rc == 0 else 'aq_engine') + '">\n'
			queries_log += '<AQL>\n'
			queries_log += aql_query + '\n'
			queries_log += '</AQL>\n'
			queries_log += '<SQL>\n'
			queries_log += sql_query + '\n'
			queries_log += '</SQL>\n'
			queries_log += '<Results nb="' + str(len(aql_rows)) + '">\n'
			for row in aql_rows:
				queries_log += '<row>' + str(row) + '</row>'
			queries_log += '\n'
			queries_log += '</Results>\n'
			queries_log += '<Expected nb="' + str(len(sql_rows)) + '">\n'
			for row in sql_rows:
				queries_log += '<row>' + str(row) + '</row>'
			queries_log += '\n'
			queries_log += '</Expected>\n'
			queries_log += '</Query>\n'

			if stop_on_failure:
				return (nb_checked, nb_error)
		else:
		
			sum_rows += len(sql_rows)
			sql_sum_time += sql_time
			aql_sum_time += aql_time
			
			if verbose:
				print_query(sys.stdout, 'query successfully checked', aql_query, sql_query, aql_rows, sql_rows)
			else:
				sys.stdout.write('.')
			#	queries_log += '<query status="successful">\n'
			#	queries_log += aql_query
			#	queries_log += '</query>\n'
	return (nb_checked, nb_error, queries_log, sql_sum_time, aql_sum_time, sum_rows)
			
# -------------------------------------------------------------------------------
def print_query_generator(queries_file):
	if opts.verbose:
		gen = QueryGenerator.parse(queries_file)
		for op_name, values in gen.ops.iteritems():
			print op_name, values
		print '\'', gen.base, '\''
		print gen.idents
		print ''

# -------------------------------------------------------------------------------
def log_error(xml_log_file_desc, test_id, tables, rows, queries_log, nb_checked, nb_errors):
	if xml_log_file_desc is not None:
		xml_log_file_desc.write('<Test id="' + str(test_id) + '">\n')
		xml_log_file_desc.write('<Database>\n')
		xml_log_file_desc.write('<Tables>\n')
		
		for i in range(len(tables)):
			xml_log_file_desc.write('<Table name="' + tables[i] + '">\n')
		
			# TODO
			# fd = xml_log_file_desc
			# fd.write('<Rows>\n')
			# for n in xrange(len(rows[i])):
			# 	fd.write('<row>')
			# 	for c in xrange(
			# 	fd.write('</row>')
			# 
			# fd.write('</Rows>\n')
		
			xml_log_file_desc.write('<Columns>\n')
			xml_log_file_desc.write('<Column name="' + 'id' + '">\n')
			values = [ id for id in range(1, len(rows[i]) + 1) ]
			xml_log_file_desc.write(str(values))
			xml_log_file_desc.write('\n')
			xml_log_file_desc.write('</Column>\n')
			xml_log_file_desc.write('<Column name="' + 'val_1' + '">\n')
			values = [ val for val in rows[i] ]
			xml_log_file_desc.write(str(values))
			xml_log_file_desc.write('\n')
			xml_log_file_desc.write('</Column>\n')
			xml_log_file_desc.write('</Columns>\n')
			xml_log_file_desc.write('</Table>\n')

		xml_log_file_desc.write('</Tables>\n')
		xml_log_file_desc.write('</Database>\n')
		
		xml_log_file_desc.write('<Queries success="' + str(nb_checked - nb_errors) + '" errors="' + str(nb_errors) + '">\n')
		xml_log_file_desc.write(queries_log)
		xml_log_file_desc.write('</Queries>')
		xml_log_file_desc.write('</Test>')
	

# -------------------------------------------------------------------------------
if __name__ == '__main__':

	nb_checked, nb_error = 0, 0
	
	try:

		cfg_filename = 'aq_test.cfg'
		cfg = ConfigParser.SafeConfigParser()
		for i in range(1, len(sys.argv)):
			if sys.argv[i] == '--cfg':
				cfg_filename = sys.argv[i+1]
				break

		cfg.read(cfg_filename)

		opts, args = parse_option(cfg)

		#print opts
		#sys.exit(0)

		if not opts.generate_db:
			opts.nb_rows = -1

		if (opts.import_db or opts.check_db) and (opts.aq_db_path is None):
			raise Exception('You need to provide an algoquest database path (--aq-db-path)')
	
		if opts.check_db and (opts.queries_file is None):
			raise Exception('You need to provide a queries file (.aql or .gen) (--queries-file)')
	
		exec_sql = ExecuteSQL(opts.db_host, opts.db_user, opts.db_pass, opts.db_name)
		exec_aql = ExecuteAQL(opts.aq_engine_tests, opts.aq_db_path, opts.aq_db_name, opts.aq_engine) # FIXME

		#
		# Open Logging
		if opts.xml_log_file is not None:
			if opts.verbose:
				print 'open', opts.xml_log_file
			xml_log_file_desc = open(opts.xml_log_file, 'w')
		
		#
		# Logging
		if xml_log_file_desc is not None:
			xml_log_file_desc.write('<Tests>\n')

		#
		# Database Generator
		db_gen = DataBaseGenerator.DBGen(opts.nb_tables, opts.nb_rows, opts.min_value, opts.max_value, 
																		 opts.all_values, opts.generate_mode, exec_sql)
		# db_gen = LogAnalyzer.DBGen(exec_sql, './aq_test_log_2_tables_errors.xml') # FIXME

		#
		# for each database generated
		test_id = 0
		for (tables, rows) in db_gen.iterate():
		
			test_id += 1
			print 'test', test_id, [ [tables[i], min(rows[i]), max(rows[i])] for i in range(len(tables)) ]
			
			if opts.verbose:
				print ''
				print '==='
				print ''

			#
			# Clean and Import
			if opts.import_db:
				if opts.verbose:
					print "Clean AQ Database", opts.aq_db_path + '/' + opts.aq_db_name
				AQImport.clean_aq_database(opts.aq_db_path, opts.aq_db_name)
			
				if opts.verbose:
					print "Import", opts.db_name, "into", opts.aq_db_path + '/' + opts.aq_db_name
				AQImport.import_aq_database(opts, force=True)

			#
			# Check Queries on Database (compare with the source)
			if opts.check_db:

				if opts.verbose:
					print "Check", opts.aq_db_name, "database"

				(c, e, queries_log, sql_time, aql_time, sum_rows) = check_database(opts.queries_file, exec_sql, exec_aql, opts.stop_on_failure, opts.verbose)
				nb_checked += c
				nb_error += e

				if e > 0:
					log_error(xml_log_file_desc, test_id, tables, rows, queries_log, c, e)

				if (e > 0) and opts.verbose:
					print 'Database Contents:'
					print ''
					DataBaseGenerator.print_base(tables, rows)
					print ''
		
				if (e > 0) and opts.stop_on_failure:
					print 'STOP ON FAILURE'
					break

				print ''
				print c, 'queries checked { success :', c - e, ' ; error :', e, ' }'
				print 'average time (seconds):', '[MySQL :', sql_time / c, '] [AlgoQuest :', aql_time / c, ']'
				print 'average result set size:', sum_rows / c
				print ''
				
			# break
					
		#
		# close Logging
		if xml_log_file_desc is not None:
			xml_log_file_desc.write('</Tests>\n')
			xml_log_file_desc.close()

	except AQImport.ImportError, e:
		print >> sys.stderr, "Error: %s" % (e.message)
		
	print ''		
	print nb_checked, 'queries checked { success :', nb_checked - nb_error, ' ; error :', nb_error, ' }'
	print ''
