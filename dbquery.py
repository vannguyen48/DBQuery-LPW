#!/usr/bin/env python

import argparse
import configparser
import csv
import datetime
import fnmatch
import json
import mysql.connector
import os
import paramiko
import sys
import time
from sshtunnel import SSHTunnelForwarder

"""
First, fill out configuration information and save queries as an .sql files (one query per sql file)

To run:
python3 dbquery.py sql_directory database_schema path/to/configfile

"""

def main():
    """
    Main Function
    """
    parse()

def ssh(processed, database, conf):
    """
    Connects to server and creates connection to the database on that server
    Performs the query, and writes result into a csv file

    Args:
        processed: sql file containing query
        database: database schema in the query
        conf: configuration files
    """
    with open(conf, 'r') as f:
        data=json.load(f)
    ssh=paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(str(data["DBs"][database]["host"]), username=str(data["DBs"][database]["user"]), password=str(data["DBs"][database]["pwd"]))

    server=SSHTunnelForwarder(
    ssh_host=str(data["DBs"][database]["host"]),
    ssh_username=str(data["DBs"][database]["user"]),
    ssh_password=str(data["DBs"][database]["pwd"]),
    remote_bind_address=(str(data["DBs"][database]["dbhost"]), 3306)
    )

    server.start()
    print(server.local_bind_port)
    con=mysql.connector.connect(host=str(data["DBs"][database]["dbhost"]),
                           user=str(data["DBs"][database]["dbuser"]),
                           password=str(data["DBs"][database]["dbpassword"]),
                           db=str(data["DBs"][database]["dbname"]),
                           port=server.local_bind_port)

    rs=con.cursor()
    for i in processed:
        query = open(i, 'r').read()
        query = query.split('\n', 2)[2]
        rs.execute(query)
        csvname = processed[i]
        rows = rs.fetchall()
        result = open(csvname, 'w')
        field_names = [i[0] for i in rs.description]
        ofile  = csv.writer(result)
        ofile.writerow(field_names)
        ofile.writerows(rows)
        result.close()
    rs.close()
    con.close()
    server.stop()
    ssh.close()

def parse():
    """
    Parses command line arguments. From command line, include directory to database queries,
    database schema, and path to server and database configuration
    """

    parser=argparse.ArgumentParser(description='Script to perform query on a server and save as CSV file (with local port forwarding)')
    parser.add_argument('SQL_PATH', type=str,
                  help='the path to the directory where files or SQL scripts are located.')
    parser.add_argument('DB', type=str,
                  help='database to query.')
    parser.add_argument('CONF', type=str,
                  help='name of JSON config file.')
    args=parser.parse_args()
    assert os.path.isdir(args.SQL_PATH), 'SQL path is invalid'
    assert os.path.isfile(args.CONF), 'Config file does not exist'

    if (args.SQL_PATH):
        process_files(args.SQL_PATH, args.DB, args.CONF)

def process_files(basepath, database, conf):
    """
    Processes files in sql query directory and generate equivalent output
    name for query result sql_files
    """

    sql_files={}
    base_head=os.path.dirname(basepath)
    for path, dirs, files in os.walk(basepath):
        for f in fnmatch.filter(files, '*.sql'):
            str=path.split(base_head)
            sql_path=os.path.join(path, f)
            oname=name_generator(f)
            opath=base_head + str[1] + '/' + oname
            sql_files[sql_path]=opath
    ssh(sql_files, database, conf)

def name_generator(filename):
    """
    Generates output name based on existing file names
    Arg:
        filename: sql file name
    Returns:
        fname: output file name (.csv extension)
    """

    name_prefix=datetime_generator()
    name=filename.split(".")
    name_suffix=name[0]
    fname=name_prefix + '_' + name_suffix + ".csv"
    return fname

def datetime_generator():
    """
    Supplements name_generator
    Returns:
        dt: current datetime string
    """

    dt=datetime.datetime.now().strftime("%y%m%d%H%M")
    return dt

if __name__ == '__main__':
    """
    Performs a main check so that this script can run on its own (rather than being imported from another module).

    """
    main()
