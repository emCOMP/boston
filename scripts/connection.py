from pymongo import MongoClient
import MySQLdb
import db_info as DB

class dbConnection(object):

    def __init__(self):
       self.m_connections = {}
       self.sql_connections = {}

    def create_mongo_connections(self,mongo_options=[]):
        if 'boston' in mongo_options:
            self.m_connections['boston'] = MongoClient(host=DB.mongo['host']).boston.tweets

        if 'new_boston' in mongo_options:
            self.m_connections['new_boston'] = MongoClient(host=DB.mongo['host']).new_boston.tweets

        if 'gnip_boston' in mongo_options:
            self.m_connections['gnip_boston'] = MongoClient(host=DB.mongo['host']).gnip_boston.tweets

        if 'iconference' in mongo_options:
            self.m_connections['iconference'] = MongoClient(host=DB.mongo['host']).iconference.tweets

    def create_sql_connections(self,sql_options=[]):
        for db_name in sql_options:
            sql_db = MySQLdb.connect(host=DB.sql['host'],
                                     user=DB.sql['user'],
                                     passwd=DB.sql['password'],
                                     db=db_name)
            self.sql_connections[db_name] = sql_db.cursor()
