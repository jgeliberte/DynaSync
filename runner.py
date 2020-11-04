import os
import time
import sys
import configparser
from datetime import datetime as dt
import argparse
from pprint import pprint
import traceback
import threading
import dbmanager as db
import threader as th

class Args:
    def get_args(self):
        parser = argparse.ArgumentParser(
            description="Database Syncer for CBEWS-L Softwares [-options]")
        parser.add_argument("-s", "--server",
                            help="Deployed server")
        try:
            args = parser.parse_args()
            return args
        except IndexError:
            print('>> Error in parsing arguments')
            error = parser.format_help()
            print(error)
            sys.exit()

class Syncer:
    def __init__(self):
        print("Syncer")

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config/config.cnf')
    init = Args()
    deployed_server = init.get_args()
    # thread = Threader(config['SYNC_CONFIG']['THREAD_COUNT'])
    # syncer = Syncer()
    dbmanager = db.DBManager(deployed_server.server)
    db_tables = dbmanager.getTables()
    for database in db_tables:
        status = dbmanager.checkTriggers(database, db_tables[database])
    # print("Script Args:", init.get_args())