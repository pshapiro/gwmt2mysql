import shutil
import gwmt.downloader
import glob
import os
import re
import csv
import time
import collections
import MySQLdb
import warnings


dbUser = '' // MySQL Username
dbPassword = '' // MySQL Password
dbHost = 'localhost' // MySQL Host
dbPort = 3306 // MySQL Host Port
dbSchema = '' // MySQL Database Name

email = '' // Google Webmaster Tools Username
emailPassword = '' // Google Webmaster Tools Password
siteUrl = '' // Google Webmaster Tools Domain (format: http://domain.com)

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

def downloadCsvs(email,password,siteUrl):
    downloader = gwmt.downloader.Downloader()
    downloader.LogIn(email,password)
    downloader.DoDownload(siteUrl,['TOP_PAGES', 'TOP_QUERIES'])


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

downloadCsvs(email, emailPassword, siteUrl)
convertLongFileNames()

removeChangeAndCtrColumns('TOP_QUERIES.csv')
addDateColumn('TOP_QUERIES.csv')

removeChangeAndCtrColumns('TOP_PAGES.csv')
addDateColumn('TOP_PAGES.csv')

putCsvToDb('TOP_PAGES.csv',dbUser,dbPassword,dbHost,dbPort,'TOP_PAGES',dbSchema)
putCsvToDb('TOP_QUERIES.csv',dbUser,dbPassword,dbHost,dbPort,'TOP_QUERIES',dbSchema)

os.remove('TOP_PAGES.csv')
os.remove('TOP_QUERIES.csv')
