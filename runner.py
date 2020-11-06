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
    def __init__(self, dbmanager, deployed_server):
        while(True):
            logs = dbmanager.fetchHistory()
            for log in logs:
                data = logs[log]
                history_data = dbmanager.fetchHistoryReferenceData(log, data)
                sync_status = dbmanager.applyHistoryChanges(history_data, data[4], data[3], data[2])
                if (sync_status == True):
                    dbmanager.confirmSyncing(data[0], deployed_server)
                sys.exit(0)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config/config.cnf')
    init = Args()
    deployed_server = init.get_args()
    dbmanager = db.DBManager(deployed_server.server)
    db_tables = dbmanager.getTables()
    for database in db_tables:
        status = dbmanager.checkTriggers(database, db_tables[database])

    print("\n\n>>-----------------------------------------")
    print(">>              Triggers set")
    print(">>-----------------------------------------\n\n")

    syncer = Syncer(dbmanager, deployed_server.server)

    # print("Script Args:", init.get_args())