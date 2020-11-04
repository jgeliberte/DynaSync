import os
import time
import sys
import configparser
from datetime import datetime as dt
import argparse
from pprint import pprint
import traceback

class Args:
    def get_args(self):
        parser = argparse.ArgumentParser(
            description="Database Syncer for CBEWS-L Softwares [-options]")
        parser.add_argument("-s", "--server",
                            help="smsinbox table (loggers or users)")
        try:
            args = parser.parse_args()
            return args
        except IndexError:
            print('>> Error in parsing arguments')
            error = parser.format_help()
            print(error)
            sys.exit()

class Threader:

    def __init__(self):
        print("initialize script")

class Syncer:

    def __init__(self):
        print("Syncer")

class DBManager:
    def __init__(self):
        print("Database Manager")

if __name__ == "__main__":

    init = Args()
    thread = Threader()
    syncer = Syncer()
    dbmanager = DBManager()
    print("Script Args:", init.get_args())