from connection import dbConnection
from collections import Counter

# Import the geographic coordinates from Kate's SQL format into mongo
# Takes SQL and Mongo database names as parameters

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

# Import the place code from Kate's SQL format into mongo
# NOT WORKING

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

# Import the author screen_name from Kate's SQL format into mongo
# takes SQL and mongo database names

def author_code_import(mongodb,sqldb):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[mongodb])
    db.create_sql_connections(sql_options=[sqldb])
    written_ids = open('written_ids_proposal.txt','w')

    #sql db query
    query = "select id,author from tweets"
    db.sql_connections['boston'].execute(query)
    for x in db.sql_connections[sqldb]:
        query = str(x[0])
        value1 = str(x[1])
        print query,value1
        written_ids.write('"%s","%s"\n' % (query,value1))
        db.m_connections[mongodb].update({'user.id':query},
                      {'$set':{
                          'user.screen_name':value1,
                          }
                      })

# Update the rumor codes in mongo for a given rumor

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

# find the intersection between 2 mongo databases
# ID field must be cast for correct comparison

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

# mark the intersection between tweets in both mongo databases
# 0 => exists in both
# 1 => only in new_boston
# 2 => only in gnip_boston, same key words as new_boston
# 3 => only in gnip_boston, different key words than new_boston

def create_instersection_codes(db1,db2,gnip_first=True):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db1])
    db.create_mongo_connections(mongo_options=[db2])

    raw_data = db.m_connections[db1].find()

    if gnip_first is True:
        for x in raw_data:
            new_data = db.m_connections[db2].find_one({'id':str(x['id'])})
            if new_data != None:
                db.m_connections[db1].update({x['id']},
                                             {'$set':
                                              {'intersect':0}
                                          })
                db.m_connections[db2].update({str(x['id'])},
                                             {'$set':
                                              {'intersect':0}
                                          })
            else:
                new_kw = ['watertown','mit']
                if (set(new_kw) & set(x['track_kw']['mentions'])) or (set(new_kw) & set(x['track_kw']['hashtags'])) or (set(new_kw) & set(x['track_kw']['text'])):
                        db.m_connections[db1].update({x['id']},
                                             {'$set':
                                              {'intersect':3}
                                          })
                else:
                    db.m_connections[db1].update({x['id']},
                                             {'$set':
                                              {'intersect':2}
                                          })
    else:
        for x in raw_data:
            new_data = db.m_connections[db2].find_one({'id':long(x['id'])})
            if new_data != None:
                db.m_connections[db1].update({str(x['id'])},
                                             {'$set':
                                              {'intersect':0}
                                          })
                db.m_connections[db2].update({x['id']},
                                             {'$set':
                                              {'intersect':0}
                                          })
            else:
                db.m_connections[db1].update({str(x['id'])},
                                             {'$set':
                                              {'intersect':1}
                                          })

if __name__ == "__main__":
    code_update_mongo_to_sql(mongodb='new_boston',
                             sqldb='craft_seals',
                             table='tweets_seals',
                             rumor='seals/craft')
