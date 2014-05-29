from connection import dbConnection
from collections import Counter
from datetime import datetime,timedelta

def geo_code_import(mongodb,sqldb):
    db = dbConnection()
    print mongodb,sqldb
    db.create_mongo_connections(mongo_options=[mongodb])
    db.create_sql_connections(sql_options=[sqldb])
    written_ids = open('written_ids_proposal.txt','w')

    #sql db query
    query = "select id,meta_gps_lat,meta_gps_long from tweets where meta_gps_lat != '' and meta_gps_long != ''"
    db.sql_connections['boston'].execute(query)
    for x in db.sql_connections['boston']:
        query = str(x[0])
        value1 = str(x[1])
        value2 = str(x[2])
        print query,value1,value2
        written_ids.write('"%s","%s","%s"\n' % (query,value1,value2))
        db.m_connections['new_boston'].update({'user.id':query},
                      {'$set':{
                          'place':{
                              'coordinates':{
                                  'type':'Point',
                                  'coordinates':[value2,value1]
                              }
                          }
                      }
                   })

def place_code_import(mongodb,sqldb):
    db = dbConnection()
    print mongodb,sqldb
    db.create_mongo_connections(mongo_options=[mongodb])
    db.create_sql_connections(sql_options=[sqldb])
    written_ids = open('written_ids_proposal.txt','w')

    #sql db query
    query = "select id,place,place_url from tweets where place != '' limit 2"
    db.sql_connections['boston'].execute(query)
    for x in db.sql_connections['boston']:
        query = str(x[0])
        value1 = str(x[1])
        value2 = str(x[2])
        print query,value1,value2
        written_ids.write('"%s","%s","%s"\n' % (query,value1,value2))
        db.m_connections['new_boston'].update({'user.id':query},
                      {'$set':{
                          'place.full_name':value1,
                          'place.url':value2,
                          }
                      })

def code_update_mongo_to_sql(mongodb,sqldb,table,rumor):
    db = dbConnection()
    print mongodb,sqldb
    db.create_mongo_connections(mongo_options=[mongodb])
    db.create_sql_connections(sql_options=[sqldb])
    written_ids = open('girl_running_update_log.txt','w')

    #sql db query
    query = "select id,code from %s" % table
    db.sql_connections[sqldb].execute(query)

    for x in db.sql_connections[sqldb].fetchall():
        query = str(x[0])
        value = str(x[1])
        print query,value
        written_ids.write('"%s","%s"\n' % (query,value))
        db.m_connections[mongodb].update({'user.id':query,
                                           'codes.rumor':rumor},
                                          {'$set':{'codes.$.code':value,}})

def total_intersection(db1,db2):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db1])
    db.create_mongo_connections(mongo_options=[db2])

    raw_data = db.m_connections[db1].find()
    count = 0

    for x in raw_data:
        new_data = db.m_connections[db2].find_one({'id':str(x['id'])})
        if new_data != None:
            count += 1

    result = '%s in %s: %s' % (db1,db2,count)
    print result

def time_update(interval,db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    raw_data = db.m_connections[db_name].find()

    for x in raw_data:
        tweet_id = x['id']
        new_time = x['created_ts'] + timedelta(hours=interval)
        db.m_connections[db_name].update({'id':tweet_id},
                                         {'$set':{'created_ts':new_time}})

def check_time(interval,db1,db2):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db1,db2])

    update_tweet = db.m_connections[db1].find()

    for x in update_tweet:
        tweet_id = x['id']
        check_tweet = db.m_connections[db2].find_one({'id':tweet_id})
        new_time = check_tweet['created_ts'] + timedelta(hours=interval)
        if new_time != x['created_ts']:
            print '%s,update' % (tweet_id)
            db.m_connections[db1].update({'id':tweet_id},
                                         {'$set':{'created_ts':new_time}})
        else:
            print '%s,pass' % (tweet_id)

if __name__ == "__main__":
    #total_intersection(db2='new_boston',db1='gnip_boston')
    #time_update(interval=-4,db_name='new_boston')
    check_time(interval=-4,db1='new_boston',db2='boston')
