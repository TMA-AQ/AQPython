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
	db_gen_options.add_option('', '--nb-rows', action="store", type="int", dest="nb_rows", default=cfg.get(section, 'nb-rows'), help="nb rows generate for each tables [default: %default]")
	db_gen_options.add_option('', '--min-value', action="store", type="int", dest="min_value", default=cfg.get(section, 'min-value'), help="minimal value generate in table t1 [default: %default]")
	db_gen_options.add_option('', '--max-value', action="store", type="int", dest="max_value", default=cfg.get(section, 'max-value'), help="maximal value generate in table t2 [default: %default]")

	#
	# misceallenous option
	parser.add_option('-v', '--verbose', action="store_true", dest="verbose", default=False, help="set verbosity")
	parser.add_option('', '--cfg', action="store", type="string", dest="cfg_file", default="", help="config filename, do not take care of other option if specified [default: %default]")

	parser.add_option_group(aq_options)
	parser.add_option_group(source_db_options)
	parser.add_option_group(check_options)
	parser.add_option_group(db_gen_options)
	
	return parser.parse_args()

# -------------------------------------------------------------------------------
def check_database(queries_file, exec_sql, exec_aql, stop_on_failure=False, verbose=False):

	if queries_file is None:
		raise Exception('you need to provide a generator queries file')

	nb_checked, nb_error = 0, 0
	for aql_query in QueryGenerator.iterate(queries_file):

		if verbose:
			print ''
			print '==='
			print ''		
			print aql_query
		
		ss = AQLParser.Statements()
		ss.parse(aql_query)
		sql_query = ss.to_sql(separator = '\n')
			
		if verbose:
			print sql_query
					
		##################################################
		# skip full outer because not supported by mysql #
		if sql_query.lower().find('full outer') != -1:
			if verbose:
				print "full outer not supported, skip query"
			continue
		##################################################
			
		nb_checked += 1

		sql_rows = exec_sql.execute_and_fetch(sql_query)
		aql_rows = exec_aql.execute(aql_query)
			
		if not util.row_comparator(sql_rows, aql_rows):
			print_query(sys.stderr, 'ERROR: query failed', aql_query, sql_query, aql_rows, sql_rows)
			nb_error += 1
			if stop_on_failure:
				return (nb_checked, nb_error)
		elif verbose:
			print_query(sys.stdout, 'query successfully checked', aql_query, sql_query, aql_rows, sql_rows)

	return (nb_checked, nb_error)
			
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
		exec_aql = ExecuteAQL('aq-engine-tests', opts.aq_db_path, opts.aq_db_name) # FIXME
		
		for (tables, rows) in DataBaseGenerator.generate(opts.nb_rows, opts.min_value, opts.max_value):
			
			if opts.generate_db:
				print "Generate database", opts.db_name, "on", opts.db_host
				DataBaseGenerator.load(exec_sql, tables, rows)
		
			if opts.import_db:
				print "Clean AQ Database", opts.aq_db_path + '/' + opts.aq_db_name
				AQImport.clean_aq_database(opts.aq_db_path, opts.aq_db_name)
			
				print "Import", opts.db_name, "into", opts.aq_db_path + '/' + opts.aq_db_name
				AQImport.import_aq_database(opts, force=True)

			if opts.check_db:
				print "Check", opts.aq_db_name, "database"
				(c, e) = check_database(opts.queries_file, exec_sql, exec_aql, opts.stop_on_failure, opts.verbose)
				nb_checked += c
				nb_error += e

				if e > 0:
					print 'Database Contents:'
					print ''
					DataBaseGenerator.print_base(tables, rows)
					print ''
		
				if opts.stop_on_failure:
					print 'STOP ON FAILURE'
					break
	
	except AQImport.ImportError, e:
		print >> sys.stderr, "Error: %s" % (e.message)
	except Exception, e:
		print >> sys.stderr, "Error: %s" % (e.message)
		
	print ''		
	print nb_checked, 'queries checked.', nb_checked - nb_error, 'success', nb_error, 'errors'
	print ''
