#! /usr/bin/python

import os, sys, getopt, time, math
from optparse import OptionParser

from ExecuteAQL import ExecuteAQL
from ExecuteSQL import ExecuteSQL
import AQLParser
import util
import QueryGenerator

def print_query(out, msg, aql_query, sql_query, aql_rows, sql_rows):
	print >> sys.stderr, ''
	print >> sys.stderr, '==========='
	print >> sys.stderr, '==========='
	print >> sys.stderr, ''
	print >> sys.stderr, msg
	print >> sys.stderr, ''
	print >> sys.stderr, '=== AQL ==='
	print >> sys.stderr, aql_query
	print >> sys.stderr, aql_rows
	print >> sys.stderr, '=== SQL ==='
	print >> sys.stderr, sql_query.strip(' \n')
	print >> sys.stderr, sql_rows

# verbose = False
# aq_db = "TEST_SMALL_ID"
# aq_path = "~/AQ_DB/"
# msql_host = "localhost"
# msql_user = "tma"
# msql_pass = "algoquest"
# msql_db_name = "algoquest"
# 
# opts, args = getopt.getopt(
# 	sys.argv[1:], 
# 	"hf:v", 
# 	["aq-db", "aq-path", "msql-host", "msql-user", "msql-pass", "msql-db-name", "help", "file=", "verbose"])
# 
# input = "stdin"
# for o, a in opts:
# 	if o in ("--aq-db"):
# 		aq_db = a		
# 	elif o in ("--aq-path"):
# 		aq_path = a		
# 	elif o in ("--msql-host"):
# 		msql_host = a		
# 	elif o in ("--msql-user"):
# 		msql_user = a		
# 	elif o in ("--msql-pass"):
# 		msql_pass = a		
# 	elif o in ("--msql-db-name"):
# 		msql_db_name = a		
# 	elif o in ("-h", "--help"):
# 		usage()
# 		sys.exit(0)
# 	elif o in ("-f", "--file"):
# 		input = a		
# 	elif o in ("-v", "--verbose"):
# 		verbose = True	
	

parser = OptionParser()

#
# algoquest database options
parser.add_option('', '--aq-db-name', 
									action="store", 
									type="string", 
									dest="aq_db_name", 
									default="algoquest", 
									help='algoquest database name [default: %default]')
parser.add_option('', '--aq-db-path', 
									action="store", 
									type="string", 
									dest="aq_db_name", 
									default="/AQ_DB/", 
									help='algoquest database path [default: %default]')
						
#
# reference database options (mysql)						
parser.add_option('', '--db-host', 
									action="store", 
									type="string", 
									dest="db_host", 
									default="localhost", 
									help='source database host [default: %default]')
parser.add_option('', '--db-user', 
									action="store", 
									type="string", 
									dest="db_user", 
									default="algoquest", 
									help='source database user [default: %default]')
parser.add_option('', '--db-pass', 
									action="store", 
									type="string", 
									dest="db_pass", 
									default="algoquest", 
									help='source database password [default: %default]')
parser.add_option('', '--db-name', 
									action="store", 
									type="string", 
									dest="db_name", 
									default="algoquest", 
									help='source database name [default: %default]')
						
# 
#						
parser.add_option('-f', '--file',
									action="store",
									type="string",
									dest="queries_file",
									help="queries file (generator or list of aql queries)")
parser.add_option('-v', '--verbose',
									action="store_true",
									dest="verbose",
									default=False,
									help="set verbosity")

opts, args = parser.parse_args()

print opts
sys.exit(0)
	
if opts.verbose:
	gen = QueryGenerator.parse(opts.queries_file)
	for op_name, values in gen.ops.iteritems():
		print op_name, values
	print '\'', gen.base, '\''
	print gen.idents
	print ''

exec_sql = ExecuteSQL(db_host, db_user, db_pass, db_name)
exec_aql = ExecuteAQL(aq_db_path, aq_db_name)
	
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
		continue
	##################################################
		
	nb_checked += 1

	sql_rows = exec_sql.execute(sql_query)
	aql_rows = exec_aql.execute(aql_query)
		
	if opts.verbose:
		print_query(sys.stdout, 'query successfully checked', aql_query, sql_query, aql_rows, sql_rows)
		
	if not util.row_comparator(sql_rows, aql_rows):
		print_query(sys.stderr, 'ERROR: query failed', aql_query, sql_query, aql_rows, sql_rows)
		nb_error += 1

print ''		
print nb_checked, 'queries checked.', nb_checked - nb_error, 'success', nb_error, 'errors'
print ''
