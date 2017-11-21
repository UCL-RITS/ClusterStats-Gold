#!/usr/bin/env python3

# Connects to a Gold sqlite3 database.

import sqlite3
import datetime
import argparse
import configparser
import csv

def getargs(argv):
    parser = argparse.ArgumentParser(description="Show allocation usage data from the Gold database")
    parser.add_argument("--institutes", help="Get usage data for all institutes", action='store_true')
    parser.add_argument("--csv", metavar="csvfile", help="Write results to csv, with the given filename")

    # Show the usage if no arguments are supplied
    if len(sys.argv[1:]) < 1:
        parser.print_usage()
        exit(1)

    # return the arguments
    # contains only the attributes for the main parser and the subparser that was used
    return parser.parse_args()
# end getargs

# Get allocation usage data for all institutes for all time.
# Order by allocation start, end and then project name 
# (as first allocation all starts on same date).
def gold_by_allocation_period_query():
    query = ("""SELECT j.g_project, sum(j.g_charge), r.g_id as date_alloc, r.g_account,
                       a.g_start_time, a.g_end_time, count(*) as num_jobs
                FROM g_job AS j
                INNER JOIN g_reservation_allocation AS r
                  ON j.g_request_id = r.g_request_id
                INNER JOIN g_allocation AS a
                  ON date_alloc = a.g_id
                GROUP BY date_alloc 
                ORDER BY a.g_start_time, a.g_end_time, j.g_project""")
    return query


# Main function
if __name__ == '__main__':

    try:
        args = getargs(argv)
        # make a dictionary from args to make string substitutions doable by key name
        args_dict = vars(args)
    except ValueError as err:
        print(err)
        exit(1)

    # read the config file    
    try:
        config = configparser.ConfigParser()
        config.read('config/gold.cnf')
        db = config['Gold']['SQLITE_DB']
    except configparser.Error as err:
        print(err)
        exit(1)

    # connect to SQLite3 database, read-only 
    try:
        conn = sqlite3.connect(db)
        cursor = conn.cursor()

        if (args.institutes):
            cursor.execute(gold_by_allocation_period_query())

        rows = cursor.fetchall()

        # may want to process data in some way before output?

        if (args.csvfile != None):
            csvwriter = csv.writer(open(csvfile, newline='', encoding='utf-8', "w"))
            csvwriter.writerows(rows)

    except (sqlite3.Error, csv.Error) as err:
        print(err)
        exit(1)
    else:
        cursor.close()
        conn.close()

# end main
