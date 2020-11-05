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
	schemas = None

	def __init__(self, server):
		print(">> Initializing Database Manager")
		credentials = DatabaseCredentials()
		if server == "pi":
			self.db_cred = credentials['RPI_CREDENTIALS']
		else:
			self.db_cred = credentials['CLOUD_CREDENTIALS']
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
				print("RESULT:", res)
				db.close()

	def fetchHistory(self):
		print(">> Fetching database history")

	def fetchHistoryReferenceData(self):
		print(">> Fetching actual data")

	def applyHistoryChanges(self):
		print("<< Applying changes")

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