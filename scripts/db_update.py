from connection import dbConnection
from collections import Counter

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
    count = Counter()

    for x in raw_data:
        new_data = db.m_connections[db2].find({'id':x['id']})
        if new_data:
            count.update(['total_overlap',
                          #raw_data['codes']['code'],
                          #raw_data['codes']['rumor']
                    ])

    result = '%s in %s: %s' % (db1,db2,count)
    print result

if __name__ == "__main__":
    total_intersection(db1='new_boston',db2='gnip_boston')
    total_intersection(db1='gnip_boston',db2='new_boston')
