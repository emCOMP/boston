from pymongo import MongoClient
import MySQLdb
import db_info as DB

class dbConnection(object):

    def __init__(self):
       self.m_connections = {}
       self.sql_connections = {}

    def create_mongo_connections(self,mongo_options=[],collections=None):
        if collections is None:
            for db_name in mongo_options:
                self.m_connections[db_name] = MongoClient(host=DB.mongo['host'])[db_name].tweets
        else:
            for db_name in mongo_options:
                for collection_name in collections[db_name]:
                    self.m_connections[db_name] = MongoClient(host=DB.mongo['host'])[db_name][collection_name]

    def create_sql_connections(self,sql_options=[]):
        for db_name in sql_options:
            sql_db = MySQLdb.connect(host=DB.sql['host'],
                                     user=DB.sql['user'],
                                     passwd=DB.sql['password'],
                                     db=db_name)
            self.sql_connections[db_name] = sql_db.cursor()
