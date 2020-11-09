import MySQLdb
import configparser
import json
import sys

class DatabaseCredentials():
    def __new__(self):
        config = configparser.ConfigParser()
        config.read('config/db.cnf')
        return config

class DBManager:

    db_cred = None
    ops_db_cred = None
    schemas = None
    deployed_server = None

    def __init__(self, server):
        print(">> Initializing Database Manager")
        credentials = DatabaseCredentials()
        self.deployed_server = server
        if server == "pi":
            self.db_cred = credentials['RPI_CREDENTIALS']
            self.ops_db_cred = credentials['CLOUD_CREDENTIALS']
        else:
            self.db_cred = credentials['CLOUD_CREDENTIALS']
            self.ops_db_cred = credentials['RPI_CREDENTIALS']
        self.schemas = credentials['DB_SCHEMAS']['schemas'].split(", ")

    def getTables(self):
        print("<< Fetching database tables")
        table_dict = {}
        for schema in self.schemas:
            try:
                db, cur = self.db_connect(schema)
                query = "SHOW TABLES;"
                res = cur.execute(query)
                out = []
                if res:
                    for row in cur.fetchall():
                        out.append(row[0])
                    db.close()
                table_dict[schema] = out
            except MySQLdb.OperationalError as err:
                print(">> Error:", err)
        return table_dict

    def checkTriggers(self, database, tables):
        print(">> Checking triggers for database:", database)
        try:
            for table in tables:
                temp_triggers = {}
                db, cur = self.db_connect(database)
                query = f"show triggers FROM {database} like '{table}';"
                res = cur.execute(query)
                out = cur.fetchall()
                if res:
                    for row in out:
                        temp_triggers[row[0]] = row
                db.close()
                self.insertTriggers(temp_triggers, database, table)
        except MySQLdb.OperationalError as err:
            print(">> Error:", err)

    def insertTriggers(self, data, database, table):
        trigger = [f'DATA_SYNC_{table}_AFTER_INSERT', f'DATA_SYNC_{table}_AFTER_UPDATE', f'DATA_SYNC_{table}_AFTER_DELETE']
        for t in trigger:
            if t in data:
                print(f"Trigger {t} already exists... Skipping...")
            else:
                print(f"<< Inserting triggers: {t}")
                if "AFTER_INSERT" in t:
                    query = f"CREATE DEFINER = CURRENT_USER TRIGGER `{database}`.`{t}` " \
                    f"AFTER INSERT ON `{table}` FOR EACH ROW BEGIN " \
                    f"INSERT INTO history_collections.history VALUES (0, new.id, 'INSERT', '{table}', '{database}', True, False);END"
                elif "AFTER_UPDATE" in t:
                    query = f"CREATE DEFINER = CURRENT_USER TRIGGER `{database}`.`{t}` " \
                    f"AFTER UPDATE ON `{table}` FOR EACH ROW BEGIN " \
                    f"INSERT INTO history_collections.history VALUES (0, new.id, 'UPDATE', '{table}', '{database}', True, False);END"
                elif "AFTER_DELETE" in t:
                    query = f"CREATE DEFINER = CURRENT_USER TRIGGER `{database}`.`{t}` " \
                    f"AFTER DELETE ON `{table}` FOR EACH ROW BEGIN " \
                    f"INSERT INTO history_collections.history VALUES (0, old.id, 'DELETE', '{table}', '{database}', True, False);END"
                else:
                    print("** Invalid key... Skipping...")
                    return -1
                db, cur = self.db_connect(database)
                res = cur.execute(query)
                db.close()

    def fetchHistory(self):
        print(">> Fetching database history")
        try:
            history_list = {}
            args = ""

            if self.deployed_server == "pi":
                args = "sync_status_cloud = False"
            elif self.deployed_server == "cloud":
                args = "sync_status_local = False"

            db, cur = self.db_connect('history_collections')

            query = f"SELECT * FROM history WHERE {args}"
            res = cur.execute(query)
            out = cur.fetchall()
            if res:
                for row in out:
                    history_list[row[0]] = row
            db.close()
            return history_list
        except MySQLdb.OperationalError as err:
            print(">> Error:", err)

    def fetchHistoryReferenceData(self, id, data):
        print(f">> Fetching actual data for history ID: {id}")
        try:
            history_data = None
            args = ""
            db, cur = self.db_connect(data[4])

            query = f"SELECT * FROM {data[4]}.{data[3]} WHERE id={data[1]}"
            res = cur.execute(query)
            out = cur.fetchall()
            if res:
                history_data = out[0]
            db.close()
            return history_data
        except MySQLdb.OperationalError as err:
            print(">> Error:", err)


    def applyHistoryChanges(self, data, schema, table, command):
        print("<< Applying changes")
        status = None
        if command == "INSERT":
            counter = 0
            value_arg = "VALUES("
            for value in data:
                if counter != 0:
                    value_arg = value_arg + f"'{value}',"
                else:
                    value_arg = value_arg + "0,"
                    counter = counter+1

            args = f"{value_arg[:-1]})"
            query = f"INSERT INTO {schema}.{table} {args}"
            try:
                db, cur = self.ops_db_connect(schema)
                execute = cur.execute(query)
                if execute == True:
                    status = True
                else:
                    status = False
                db.commit()
            except MySQLdb.OperationalError as err:
                print(err)
                status = False
            finally:
                db.close()
                return status
        elif command == "UPDATE":
            counter = 0
            value_args = "SET"
            ret_val = False
            columns = self.getColumnNames(schema, table)
            for index in range(len(columns)):
                if index != 0:
                    value_args = value_args + f" {columns[index]} = '{data[index]}',"
            args = f"{value_args[:-1]}"
            query = f"UPDATE {table} {args} WHERE id = {data[0]}"
            try:
                if self.deployed_server == "pi":
                    db, cur = self.ops_db_connect(schema)
                else:
                    db, cur = self.db_connect(schema)
                result = cur.execute(query)
                if result == 0:
                    ret_val = True
                db.commit()
            except MySQLdb.OperationalError as err:
                print(">> Error:", err)
            finally:
                db.close()
                return ret_val
        elif command == "DELETE":
            print("DELETE DATA")
        else:
            print("INVALID")

    def crossCheckData(self, data, schema):
        print(data)
        print(schema)

    def getColumnNames(self, schema, table):
        ret_val = []
        try:
            db, cur = self.db_connect(schema)
            query = f"SELECT Column_name from INFORMATION_SCHEMA.columns where Table_name = '{table}'"
            res = cur.execute(query)
            out = cur.fetchall()
            for row in out:
                ret_val.append(row[0])
        except MySQLdb.OperationalError as err:
            print(">> Error:", err)
        finally:
            db.close()
            return ret_val

    def confirmSyncing(self, history_id, deployed_server):
        if deployed_server == "pi":
            args = "sync_status_cloud = True"
        else:
            args = "sync_status_local = True"
        query = f"UPDATE history_collections.history SET {args} WHERE history_id = {history_id}"
        try:
            db, cur = self.db_connect('history_collections')
            result = cur.execute(query)
            db.commit()
        except MySQLdb.OperationalError as err:
            print(">> Error:", err)
        finally:
            db.close()

    def db_connect(self, schema):
        try:
            db = MySQLdb.connect(self.db_cred['host'],
                                 self.db_cred['user'],
                                 self.db_cred['password'], schema)
            cur = db.cursor()
            return db, cur
        except TypeError as err:
            self.error_logger.store_error_log(self.exception_to_string(err))
            print('Error Connection Value')
            return False
        except MySQLdb.OperationalError as err:
            self.error_logger.store_error_log(self.exception_to_string(err))
            print("MySQL Operationial Error:", err)
            return False
        except (MySQLdb.Error, MySQLdb.Warning) as err:
            self.error_logger.store_error_log(self.exception_to_string(err))
            print("MySQL Error:", err)
            return False

    def ops_db_connect(self, schema):
        try:
            db = MySQLdb.connect(self.ops_db_cred['host'],
                                 self.ops_db_cred['user'],
                                 self.ops_db_cred['password'], schema)
            cur = db.cursor()
            return db, cur
        except TypeError as err:
            print('Error Connection Value')
            return False
        except MySQLdb.OperationalError as err:
            print("MySQL Operationial Error:", err)
            return False
        except (MySQLdb.Error, MySQLdb.Warning) as err:
            print("MySQL Error:", err)
            return False