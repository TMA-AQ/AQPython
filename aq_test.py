#! /cygdrive/c/Python27/python

import os, sys, getopt, time, math

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

verbose = False
aq_db = "TEST_SMALL_ID"
aq_path = "E:/AQ_DATABASES/DB/"
msql_host = "localhost"
msql_user = "tma"
msql_pass = "AlgoQuest"
msql_db_name = "test"

opts, args = getopt.getopt(
	sys.argv[1:], 
	"hf:v", 
	["aq-db", "aq-path", "msql-host", "msql-user", "msql-pass", "msql-db-name", "help", "file=", "verbose"])

input = "stdin"
for o, a in opts:
	if o in ("--aq-db"):
		aq_db = a		
	elif o in ("--aq-path"):
		aq_path = a		
	elif o in ("--msql-host"):
		msql_host = a		
	elif o in ("--msql-user"):
		msql_user = a		
	elif o in ("--msql-pass"):
		msql_pass = a		
	elif o in ("--msql-db-name"):
		msql_db_name = a		
	elif o in ("-h", "--help"):
		usage()
		sys.exit(0)
	elif o in ("-f", "--file"):
		input = a		
	elif o in ("-v", "--verbose"):
		verbose = True	
	
# for v in util.permut(['. T1 ID', '. T2 ID', '. T3 ID']):
# 	print v	
# sys.exit(0)
	
if verbose:
	gen = QueryGenerator.parse(input)
	for op_name, values in gen.ops.iteritems():
		print op_name, values
	print '\'', gen.base, '\''
	print gen.idents
	print ''

exec_sql = ExecuteSQL(msql_host, msql_user, msql_pass, msql_db_name)
exec_aql = ExecuteAQL(aq_path, aq_db)
	
nb_checked, nb_error = 0, 0
for aql_query in QueryGenerator.generate(input):

	# print ''
	# print 'Select Statement : ', ss.selectStmt
	# print 'From Statement   : ', ss.fromStmt
	# print 'Where Statement  : '
	# for conds in ss.whereStmt:
	# 	print '  ', conds
	# print 'Group Statement  : ', ss.groupStmt
	# print 'Order Statement  : ', ss.orderStmt
	
	# sql_query = util.aql2sql(aql_query)
	
	print aql_query
	
	ss = AQLParser.Statements()
	ss.parse(aql_query)
	sql_query = ss.to_sql(separator = '\n')
		
	print sql_query
				
	##################################################
	# skip full outer because not supported by mysql #
	if sql_query.lower().find('full outer') != -1:
		continue
	##################################################
		
	nb_checked += 1
		
	sql_rows = exec_sql.execute(sql_query)
	aql_rows = exec_aql.execute(aql_query)
		
	if verbose:
		print_query(sys.stdout, 'query successfully checked', aql_query, sql_query, aql_rows, sql_rows)
		
	if not util.row_comparator(sql_rows, aql_rows):
		print_query(sys.stderr, 'ERROR: query failed', aql_query, sql_query, aql_rows, sql_rows)
		nb_error += 1
		
print nb_checked, 'queries checked.', nb_checked - nb_error, 'success', nb_error, 'errors'