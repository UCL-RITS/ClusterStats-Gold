#!/usr/bin/env python3

# Connects to a Gold sqlite3 database.

import sqlite3
import datetime
import argparse
import configparser
import csv
import gold_queries

def getargs(argv):
    parser = argparse.ArgumentParser(description="Show allocation usage data from the Gold database")
    parser.add_argument("--all", help="Get usage data for all institutes for all time", action='store_true')
    parser.add_argument("--start", help="Get allocations from this start date yyyy-mm-dd")
    parser.add_argument("--end", help="Get allocations until this end date yyyy-mm-dd")
    parser.add_argument("--project", help="Get usage data for projects beginning with this name only")
    parser.add_argument("--csv", metavar="csvfile", help="Write results to csv, with the given filename")
    parser.add_argument("--debug", action='store_true')

    # Show the usage if no arguments are supplied
    if len(sys.argv[1:]) < 1:
        parser.print_usage()
        exit(1)

    # return the arguments
    # contains only the attributes for the main parser and the subparser that was used
    return parser.parse_args()
# end getargs


# Main function
if __name__ == '__main__':

    try:
        args = getargs(argv)
        # make a dictionary from args to make string substitutions doable by key name
        args_dict = vars(args)
    except ValueError as err:
        print(err)
        exit(1)
    print(args)

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

        if (args.all):
            cursor.execute(gold_queries.gold_by_all_allocation_periods())

        rows = cursor.fetchall()

        # may want to process data in some way before output?

        # write to specified csv file
        if (args.csvfile != None):
            if (args.debug):
                print("Attempting to write to " + csvfile)
            csvwriter = csv.writer(open(csvfile, newline='', encoding='utf-8', "w"))
            csvwriter.writerows(rows)

    except (sqlite3.Error, csv.Error) as err:
        print(err)
        exit(1)
    else:
        cursor.close()
        conn.close()

# end main
