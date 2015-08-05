#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
To use:
1) Install the Google Python client library, as shown at https://developers.google.com/webmaster-tools/v3/libraries.
2) Sign up for a new project in the Google APIs console at https://code.google.com/apis/console.
3) Register the project to use OAuth2.0 for installed applications.
4) Copy your client ID, client secret, and redirect URL into the client_secrets.json file included in this package.
5) Run the app in the command-line as shown below.

Sample usage:

  $ python search-analytics-api-sample.py 'https://www.example.com/' '2015-05-01' '2015-05-30'
'''

import argparse
import sys
from googleapiclient import sample_tools
import shutil
import glob
import os
import re
import time
import collections
import MySQLdb
import warnings
import csv

# Declare command-line flags.
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('property_uri', type=str,
						help=('Site or app URI to query data for (inclufing '
						'trailing slash).'))
argparser.add_argument('start_date', type=str,
						help=('Start date of the requested date range in '
						'YYYY-MM-DD format.'))
argparser.add_argument('end_date', type=str,
						help=('End date of the requested date range in '
						'YYYY-MM-DD format.'))

def main(argv):
	service, flags = sample_tools.init(
		argv, 'webmasters', 'v3', __doc__, __file__, parents=[argparser],
		scope='https://www.googleapis.com/auth/webmasters.readonly')

	# Get the queries for the date range, sorted by click count, descending.
	request = {
		'startDate': flags.start_date,
		'endDate': flags.end_date,
		'dimensions': ['query'],
	}
	response = execute_request(service, flags.property_uri, request)
	print_table(response, 'Export to CSV complete')

def execute_request(service, property_uri, request):
	'''Executes a searchAnalytics.query request.

	Args:
		service: The webmasters service to use when executing the query.
		property_uri: The site or app URI to request data for.
		request: The request to be executed.

	Returns:
		An array of response rows.
	'''
	return service.searchanalytics().query(
		siteUrl=property_uri, body=request).execute()

def print_table(response, title):
	'''Prints out a response table.

	Each row contains key(s), clicks, impressions, CTR, and average position.

	Args:
		response: The server response to be printed as a table.
		title: The title of the table.
	'''
	#print title + ':'

	if 'rows' not in response:
		print 'Empty response'
		return

	rows = response['rows']
	row_format = '{:<20}' + '{:>20}' * 4
	# print row_format.format('Keys', 'Clicks', 'Impressions', 'CTR', 'Position')
	f = open("./TOP_QUERIES.csv", 'wt')
	writer = csv.writer(f)
	writer.writerow( ('query', 'impressions', 'clicks', 'avg_position') )
	for row in rows:
		keys = ''
		# Keys are returned only if one or more dimensions are requested.
		if 'keys' in row:
			keys = u','.join(row['keys']).encode('utf-8')
		#print row_format.format(
		#	keys, row['clicks'], row['impressions'], row['ctr'], row['position'])
		writer.writerow( (keys, row['impressions'], row['clicks'], row['position']) )
	f.close()

# Fill out with your MySQL database information

dbUser = '' // MySQL Username
dbPassword = '' // MySQL Password
dbHost = 'localhost' // MySQL Host
dbPort = 3306 // MySQL Host Port
dbSchema = '' // MySQL Database Name

#based on https://bitbucket.org/richardpenman/csv2mysql
# suppress annoying mysql warnings
warnings.filterwarnings(action='ignore', category=MySQLdb.Warning)

def get_type(s):
    """Find type for this string
    """
    # try integer type
    try:
        v = int(s)
    except ValueError:
        pass
    else:
        if abs(v) > 2147483647:
            return 'bigint'
        else:
            return 'int'
    # try float type
    try:
        float(s)
    except ValueError:
        pass
    else:
        return 'double'

    # check for timestamp
    dt_formats = (
        ('%Y-%m-%d %H:%M:%S', 'datetime'),
        ('%Y-%m-%d %H:%M:%S.%f', 'datetime'),
        ('%Y-%m-%d', 'date'),
        ('%H:%M:%S', 'time'),
    )
    for dt_format, dt_type in dt_formats:
        try:
            time.strptime(s, dt_format)
        except ValueError:
            pass
        else:
            return dt_type

    # doesn't match any other types so assume text
    if len(s) > 255:
        return 'text'
    else:
        return 'varchar(255)'


def most_common(l):
    """Return most common value from list
    """
    # some formats trump others
    for dt_type in ('text', 'bigint'):
        if dt_type in l:
            return dt_type
    return max(l, key=l.count)


def get_col_types(input_file, max_rows=1000):
    """Find the type for each CSV column
    """
    csv_types = collections.defaultdict(list)
    reader = csv.reader(open(input_file))
    # test the first few rows for their data types
    for row_i, row in enumerate(reader):
        if row_i == 0:
            header = row
        else:
            for col_i, s in enumerate(row):
                data_type = get_type(s)
                csv_types[header[col_i]].append(data_type)

        if row_i == max_rows:
            break

    # take the most common data type for each row
    return [most_common(csv_types[col]) for col in header]


def get_schema(table, header, col_types):
    """Generate the schema for this table from given types and columns
    """
    schema_sql = """CREATE TABLE IF NOT EXISTS %s (
        id int NOT NULL AUTO_INCREMENT,""" % table

    for col_name, col_type in zip(header, col_types):
        schema_sql += '\n%s %s,' % (col_name, col_type)

    schema_sql += """\nPRIMARY KEY (id)
        ) DEFAULT CHARSET=utf8;"""
    return schema_sql


def get_insert(table, header):
    """Generate the SQL for inserting rows
    """
    field_names = ', '.join(header)
    field_markers = ', '.join('%s' for col in header)
    return 'INSERT INTO %s (%s) VALUES (%s);' % \
        (table, field_names, field_markers)


def safe_col(s):
    return re.sub('\W+', '_', s.lower()).strip('_')


def putCsvToDb(input_file, user, password, host, port, table, database):
    print "Importing `%s' into MySQL database `%s.%s'" % (input_file, database, table)
    db = MySQLdb.connect(host=host, user=user, passwd=password, port = port)
    cursor = db.cursor()
    # create database and if doesn't exist
    cursor.execute('CREATE DATABASE IF NOT EXISTS %s;' % database)
    db.select_db(database)

    # define table
    print 'Analyzing column types ...'
    col_types = get_col_types(input_file)
    print col_types

    header = None
    for row in csv.reader(open(input_file)):
        if header:
            cursor.execute(insert_sql, row)
        else:
            header = [safe_col(col) for col in row]
            schema_sql = get_schema(table, header, col_types)
            print schema_sql
            # create table
            #cursor.execute('DROP TABLE IF EXISTS %s;' % table)
            cursor.execute(schema_sql)
            # create index for more efficient access
            try:
                cursor.execute('CREATE INDEX ids ON %s (id);' % table)
            except MySQLdb.OperationalError:
                pass # index already exists

            print 'Inserting rows ...'
            # SQL string for inserting data
            insert_sql = get_insert(table, header)

    # commit rows to database
    print 'Committing rows to database ...'
    db.commit()
    print 'Done!'

def convertLongFileNames():
    os.chdir(".")
    files =glob.glob("*.csv")
    for file in files:
        if 'TopSearchUrls' in file:
            shutil.copyfile(file, 'TOP_PAGES.csv')
            os.remove(file)

        if 'TopSearchQueries' in file:
            shutil.copyfile(file, 'TOP_QUERIES.csv')
            os.remove(file)


def removeChangeAndCtrColumns(filename):
    with open(filename,"rb") as source:
        rdr= csv.reader( source )
        with open("temp","wb") as result:
            wtr = csv.writer( result )
            for r in rdr:
                wtr.writerow( (r[0], r[1], r[3], r[7]) )
    shutil.copyfile("temp", filename)
    os.remove("temp")

def addDateColumn(filename):
    with open(filename,'r') as source:
        with open('temp', 'w') as result:
            writer = csv.writer(result, lineterminator='\n')
            reader = csv.reader(source)

            all = []
            row = next(reader)
            row.append('Date')
            all.append(row)

            for row in reader:
                row.append(time.strftime("%Y-%m-%d"))
                all.append(row)

            writer.writerows(all)
    shutil.copyfile("temp", filename)
    os.remove("temp")

if __name__ == '__main__':
	main(sys.argv)
	
convertLongFileNames()

addDateColumn('TOP_QUERIES.csv')

putCsvToDb('TOP_QUERIES.csv',dbUser,dbPassword,dbHost,dbPort,'TOP_QUERIES',dbSchema)

os.remove('TOP_QUERIES.csv')