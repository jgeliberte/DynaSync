import MySQLdb
import configparser
import json

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
		print(">> Checking triggers")

	def insertTriggers(self):
		print("<< Inserting triggers")

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