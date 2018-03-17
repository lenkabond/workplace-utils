#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re, string
import os, sys
import psycopg2
import datetime
import csv
import ConfigParser
import argparse
import traceback
from sys import stdout, stderr

def get_arguments():
    '''parse command-line arguments to find out whether to run in a verbose mode'''
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="print the SQL queries that the programs runs", dest='verbose', action='store_true', default=False)
    args = parser.parse_args()
    return args.verbose
    

def create_connection(host, username, password):
    '''connect to the database  '''
    params = {
        'dbname': 'mimes_rep',
        'user': username,
        'password': password,
        'host': host,
        'port': 5433
        }
    conn = psycopg2.connect(**params)
    return conn

def try_connect(login_data):
    ''' try to connect to the database, quit and notify of the error if unsuccessful '''
    try:
        conn = create_connection(login_data[0], login_data[1], login_data[2])    
    except:
        traceback.print_exc(file=stderr)
        stderr.write('Could not connect to the database.\n')
        exit(1)
    return conn


def parse_basic_config(config_filename):
    '''take the config file and return the config object and parsed login data '''
    if not os.path.isfile(config_filename):
        stderr.write('Config file does not exist in this directory.\n')
        exit(1)
    config = ConfigParser.ConfigParser()
    config.read(config_filename)
    host = config.get('LOGIN DATA', 'host')
    username = config.get('LOGIN DATA', 'username')
    password = config.get('LOGIN DATA', 'password')
    return (config, (host, username, password))


def parse_interval_config(config_filename):
    '''take the config file and return the config object and parsed login and interval data '''
    config, login_data = parse_basic_config(config_filename)
    interval = config.getint('INTERVAL', 'days')
    offset = config.getint('INTERVAL', 'offset')
    return (config, login_data, interval, offset)


def get_list_from_file(filename):
    if not os.path.isfile(filename):
        stderr.write(filename + ' does not exist in this directory. Please check your config file and/or the file with the list of rules.\n')
        exit(1)
    rules = []
    with open(filename, 'r') as rulesfile:
        for line in rulesfile:
            rules.append(line.strip('\n'))
    return rules


def form_mimes_table_name(now, i, offset):
    '''create the name of the table to query in the format "mimes17_12_31" '''
    query_day = now - datetime.timedelta(days = 1 + i + offset)
    database_id = query_day.strftime("%y_%m_%d")
    return "mimes" + database_id


def form_stats_tablename(now, i):
    '''create the name of the table to query in the format "quarterheaderstats18_1" '''
    query_day = now - datetime.timedelta(days = 25 + i * 91)
    year = query_day.strftime("%y")
    quarter = str((query_day.month - 1) // 3 + 1)
    return "quarterheaderstats" + year + '_' + quarter


def form_condition(prop, values_list):
    ''' form an sql condition using a property and a list of its possible values '''
    values_regex = ''
    for value in values_list:
        escaped_value = re.escape(value)
        values_regex = values_regex + escaped_value + '|'
    values_regex = values_regex.strip('|')
    return ' WHERE ' + prop + " ~ '" + values_regex + "'\n"



def get_column_names(select):
    '''parse select statement for the names of the columns for the .csv file '''
    select_list = select.split('\n')
    column_list = []
    for line in select_list:
        line = string.strip(line, ',')
        index = string.rfind(line, ' as ')
        try:
            column_list.append(line[ index + 4:])
        except:
            traceback.print_exc(file=stderr)
            stderr.write("Could not parse the select statement for column names. Please include ' as ' for each column. \n")
            exit(1) 
    return column_list


def try_execute_query(query):
    ''' make an attempt to execute query. If it fails, notify the user, close the connection and exit '''
    try:
        cur.execute(query)
    except:
        traceback.print_exc(file=stderr)
        cur.close()
        conn.close()
        exit(1)
    return cur


def store_query_results(cur, writer):
    ''' store the results to mimes table in a .csv file using a csv.writer '''
    for record in cur:
        # values that contain "; " make the .csv file ugly unless taken care of
        record_list = []
        for value in record: 
            if isinstance(value, basestring):
                if value.find('; ') > -1: 
                    value = str(value.split('; '))
            record_list.append(value)
        writer.writerow(record_list)
    stdout.write(rule_filename + " has been updated.\n") 


