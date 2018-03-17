#!/usr/bin/env python
import re, string
import os, sys
import psycopg2
import datetime
import csv
import ConfigParser
import argparse
import traceback
from sys import stdout, stderr
import threading

import dbfunctions
 

def parse_trackrule_config():
    '''parse trackrules.cfg using a function from dbfunctions '''
    config, login_data, interval, offset = dbfunctions.parse_interval_config('trackrules.cfg')
    key_element = config.get('KEY ELEMENT', 'element')
    rules = config.get('ELEMENT VARIANTS', 'variants')
    rules_list = rules.split('\n')
    select = config.get('QUERY', 'select')
    conditions = config.get('QUERY', 'conditions')
    statements = config.get('QUERY', 'other_statements')
    return (login_data, rules_list, interval, offset, (key_element, select, conditions, statements))


def write_mimes_query(table_name, rule_name, query_data):
    '''create the SQL query'''
    key_element, select, conditions, statements = query_data
    query = 'SELECT ' + select
    query += '\nFROM ' + table_name
    query += '\nWHERE ' + key_element
    query += " like '%" + rule_name + "%' " 
    query += conditions
    query += "\n" + statements
    return query

    
def write_rule_csv(conn, rule_name, interval, offset, query_data, now, verbose):
    '''for each rule create a separate .csv file, get all the data from boson and write it there '''
    cur = conn.cursor()
    rule_filename = rule_name + '.csv'
    with open(rule_filename, 'a') as results_file:
        writer = csv.writer(results_file)
        columns = dbfunctions.get_column_names(query_data[1])
        writer.writerow(columns)
        for i in range(interval):
            table_name = dbfunctions.form_mimes_table_name(now, i, offset)
            query = write_mimes_query(table_name, rule_name, query_data)
            if verbose:
                print query
            else:
                print "Executing query for " + rule_name + " on " + table_name 
            cur = dbfunctions.try_execute_query(query)
            dbfunctions.store_query_results(cur, writer)
    cur.close()


class ruleThread (threading.Thread):
    def __init__(self, conn, rule_name, interval, offset, query_data, now, verbose):
        threading.Thread.__init__(self)
        self.conn = conn
        self.rule_name = rule_name
        self.interval = interval
        self.offset = offset
        self.query_data = query_data
        self.now = now
        self.verbose = verbose
    def run(self):
        write_rule_csv(self.conn, self.rule_name, self.interval, self.offset, self.query_data, self.now, self.verbose)
    
def main():
    now = datetime.datetime.now()
    verbose = dbfunctions.get_arguments()
    login_data, rules, interval, offset, query_data = parse_trackrule_config()
    conn = dbfunctions.try_connect(login_data)
    threads = []
    for rule in rules:
        thread = ruleThread(conn, rule, interval, offset, query_data, now, verbose)
        thread.start()
        threads.append(thead)
    for t in threads:
        t.join()
    conn.close()


if __name__ == "__main__":
    main()
    