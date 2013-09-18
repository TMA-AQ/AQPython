#! /usr/bin/python

import os, sys, getopt, time, math
from optparse import OptionParser, OptionGroup

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
def parse_option():
	parser = OptionParser()

	#
	# algoquest database options
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
	# reference database options (mysql)		
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
							
	# 
	#			
	check_options = OptionGroup(parser, "check options")			
	check_options.add_option('-f', '--file',
													action="store",
													type="string",
													dest="queries_file",
													help="mandatory queries file (generator or list of aql queries)")
	check_options.add_option('-v', '--verbose',
													action="store_true",
													dest="verbose",
													default=False,
													help="set verbosity")
	check_options.add_option('-s', '--stop-on-failure',
													action="store_true",
													dest="stop_on_failure",
													default=False,
													help="stop testing when an error occur")

	parser.add_option_group(aq_options)
	parser.add_option_group(source_db_options)
	parser.add_option_group(check_options)
	
	return parser.parse_args()

# -------------------------------------------------------------------------------
def check_database(opts, exec_sql, exec_aql):

	if opts.queries_file is None:
		raise Exception('you need to provide a generator queries file')

	nb_checked, nb_error = 0, 0
	for aql_query in QueryGenerator.generate(opts.queries_file):

		if opts.verbose:
			print ''
			print '==='
			print ''		
			print aql_query
		
		ss = AQLParser.Statements()
		ss.parse(aql_query)
		sql_query = ss.to_sql(separator = '\n')
			
		if opts.verbose:
			print sql_query
					
		##################################################
		# skip full outer because not supported by mysql #
		if sql_query.lower().find('full outer') != -1:
			if opts.verbose:
				print "full outer not supported, skip query"
			continue
		##################################################
			
		nb_checked += 1

		sql_rows = exec_sql.execute_and_fetch(sql_query)
		aql_rows = exec_aql.execute(aql_query)
			
		if not util.row_comparator(sql_rows, aql_rows):
			print_query(sys.stderr, 'ERROR: query failed', aql_query, sql_query, aql_rows, sql_rows)
			nb_error += 1
			if opts.stop_on_failure:
				raise Exception('STOP ON FAILURE')
		elif opts.verbose:
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
		
	opts, args = parse_option()
			
	nb_checked, nb_error = 0, 0
	
	exec_sql = ExecuteSQL(opts.db_host, opts.db_user, opts.db_pass, opts.db_name)
	exec_aql = ExecuteAQL(opts.aq_db_path, opts.aq_db_name)
	
	try:
		
		for (tables, tuples) in DataBaseGenerator.generate(10, 1, 10):
			
			print "Generate database"
			DataBaseGenerator.load(exec_sql, tables, tuples)
			
			print "Clean AQ Database", opts.aq_db_path + '/' + opts.aq_db_name
			AQImport.clean_aq_database(opts.aq_db_path, opts.aq_db_name)
			
			print "Import", opts.db_name, "into", opts.aq_db_path + '/' + opts.aq_db_name
			AQImport.import_aq_database(opts, force=True)
			
			(c, e) = check_database(opts, exec_sql, exec_aql)
			nb_checked += c
			nb_error += e
			
	except AQImport.ImportError, e:
		print >> sys.stderr, "Error: %s" % (e.message)
	except Exception, e:
		print >> sys.stderr, "Error: %s" % (e.message)
		
	print ''		
	print nb_checked, 'queries checked.', nb_checked - nb_error, 'success', nb_error, 'errors'
	print ''
