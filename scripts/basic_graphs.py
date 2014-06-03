from connection import dbConnection
from collections import Counter
from datetime import datetime
import utils

rumors = ['girl running','sunil','seals/craft','cell phone','proposal','jfk']
default_database = "new_boston"

def top_hashtags(db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "iconf_top_hashtags.csv"
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')

    data = db.m_connections[db_name].find({
        'counts.hashtags':{
            '$gt':0
        }
    })

    count = Counter()

    for x in data:
        count.update(x['hashtags'])

    for x in count.most_common(100):
        print x

def top_urls(db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "iconf_top_hashtags.csv"
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')

    data = db.m_connections[db_name].find({
        'counts.urls':{
            '$gt':0
        }
    })

    count = Counter()

    for x in data:
        for y in x['entities']['urls']:
            count.update([y['expanded_url']])

    for x in count.most_common(100):
        print x

def text_by_time():
    print 'enter a valid file name:'
    fname_in = raw_input('>> ')
    print 'enter a rumor: (girl running, sunil, craft/seals, cell phone, proposal, jfk)'
    rumor_in = raw_input('>> ')
    print 'enter a code (misinfo, correction, speculation, hedge, question, other/unclear/neutral):'
    code_in = raw_input('>> ')

    print 'enter a start day (15 through 22):'
    day = int(raw_input('>> '))
    print 'enter a start hour (0 through 23):'
    hour = int(raw_input('>> '))
    print 'enter a start minute (0 through 59):'
    minute = int(raw_input('>> '))
    dateStart = datetime(2013,04,day,hour,minute)

    print 'enter an end day (15 through 22):'
    day = int(raw_input('>> '))
    print 'enter an end hour (0 through 23):'
    hour = int(raw_input('>> '))
    print 'enter an end minute (0 through 59):'
    minute = int(raw_input('>> '))
    dateEnd = datetime(2013,04,day,hour,minute,59)

    _text_by_time(db_name='new_boston',
                  rumor=rumor_in,
                  fname=fname_in,
                  start_time=dateStart,
                  end_time=dateEnd,
                  code=code_in)

def rumor_over_time(rumor=False):
    rumors = ['girl running','sunil','seals/craft','cell phone','proposal','jfk']

    print 'enter a valid file name:'
    user_in = raw_input('>> ')

    if rumor == True:
        for x in rumors:
            _rumor_over_time(db_name='new_boston',rumor=x,gran=True,fname=user_in)
    else:
        _total_tweets_over_time(db_name='gnip_boston',fname=user_in)

def _rumor_over_time(db_name,rumor,gran,fname):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_%s.csv" % (rumor.replace('/','_'), fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    if gran:
        f.write('time,misinfo,correction,speculation,hedge,question,unrelated/neutral/other\n')
    else:
        f.write('time,misinfo,correction,unrelated/neutral/other\n')

    for i in range(15,23):      #15-23 (day)
        for j in range(0,24):   #0-24 (hour)
            for k in range(0,60,10):
                count = Counter()
                dateStart = datetime(2013,04,i,j,k)
                dateEnd = datetime(2013,04,i,j,(k+9),59)
                #print "time: %s,%s" % (dateStart,dateEnd)
                print "querying database..."
                raw_data = db.m_connections[db_name].find({
                    "created_ts":{
                        "$gte":dateStart,
                        "$lte":dateEnd
                    },
                    "codes.rumor":rumor
                })
                result = ''
                if raw_data.count == 0:
                    if gran:
                        result = '"%s",0,0,0,0,0,0\n'
                    else:
                        result = '"%s",0,0,0\n'
                else:
                    for x in raw_data:
                        for y in x['codes']:
                            if y['rumor'] == rumor:
                                count.update([y['code']])
                    if gran:
                        misinfo = count['misinfo']
                        speculation = count['speculation']
                        hedge = count['hedge']
                        correction = count['correction']
                        question = count['question']
                        other = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']
                        result = '"%s",%d,%d,%d,%d,%d,%d\n' % (dateStart,
                                                               misinfo,
                                                               correction,
                                                               speculation,
                                                               hedge,
                                                               question,
                                                               other)
                    else:
                        misinfo = count['misinfo'] + count['speculation'] + count['hedge']
                        correction = count['correction'] + count['question']
                        other = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']
                        result = '"%s",%d,%d,%d\n' % (dateStart,
                                                      misinfo,
                                                      correction,
                                                      other)
                print "writing data"
                f.write(result)

def _total_tweets_over_time(db_name,fname):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s.csv" % (fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('time,total tweets\n')

    for i in range(15,23):      #15-23 (day)
        for j in range(0,24):   #0-24 (hour)
            for k in range(0,60,10):
                dateStart = datetime(2013,04,i,j,k)
                dateEnd = datetime(2013,04,i,j,(k+9),59)
                #print "time: %s,%s" % (dateStart,dateEnd)
                print "querying database..."
                raw_data = db.m_connections[db_name].find({
                    "created_ts":{
                        "$gte":dateStart,
                        "$lte":dateEnd
                    }
                }).count()
                print "writing data"
                result = '"%s",%d\n' % (dateStart,raw_data)
                f.write(result)

def rumor_over_time():
    print '\n*rumor_over_time*\n\nThis method outputs tweet features for all of the data in a selection\n'
    print 'enter a valid file name:'
    user_in2 = raw_input('>> ')

    print 'would you like to data from coded data or all collected data? (0 for coded, 1 for all):'
    user_in = raw_input('>> ')
    if int(user_in) is 0:
        for x in rumors:
            _rumor_over_time(db_name=default_database,rumor=x,gran=True,fname=user_in)
    elif int(user_in) is 1:
        _total_tweets_over_time(db_name='gnip_boston',fname=user_in2)
    else:
        print "please enter a valid response"
    print "done!\nexiting..."

def _top_hashtags(db_name, fname, rumor):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_%s.csv" % (rumor.replace('/','_'), fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')
    print "querying database..."
    data = db.m_connections[db_name].find({
        'counts.hashtags':{
            '$gt':0
        }
    })

    count = Counter()
    print "writing data"
    for x in data:
        count.update(x['hashtags'])

    for x in count.most_common(100):
        print x

def top_hashtags():
    print '\n*top_hashtags*\n\nThis method outputs the 100 most common hashtags in a selection\n'
    print 'enter a valid file name:'
    user_in = raw_input('>> ')
    print 'please select a rumor (enter 0 for all):'
    i = 1
    for item in rumors:
        print '[%i] %s' % (i, item)
        i += 1
    user_select = raw_input('>> ')
    if user_select is 0:
        for x in rumors:
            _top_hashtags(db_name=default_database, fname=user_in, rumor=x)
    else:
        _top_hashtags(db_name=default_database, fname=user_in, rumor=rumors[int(user_select) - 1])
    print "done!\nexiting..."

def _top_urls(db_name, fname, rumor):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_%s.csv" % (rumor.replace('/','_'), fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')
    print "querying database..."
    data = db.m_connections[db_name].find({
        'counts.urls':{
            '$gt':0
        },
        'counts.rumor':rumor
    })

    count = Counter()
    print "writing data"
    for x in data:
        for y in x['entities']['urls']:
            count.update([y['expanded_url']])

def _total_tweets_over_time(db_name,fname):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s.csv" % (fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('time,total tweets\n')

    for i in range(15,23):      #15-23 (day)
        for j in range(0,24):   #0-24 (hour)
            for k in range(0,60,10):
                dateStart = datetime(2013,04,i,j,k)
                dateEnd = datetime(2013,04,i,j,(k+9),59)
                #print "time: %s,%s" % (dateStart,dateEnd)

                raw_data = db.m_connections[db_name].find({
                    "created_ts":{
                        "$gte":dateStart,
                        "$lte":dateEnd
                    }
                }).count()

                result = '"%s",%d\n' % (dateStart,raw_data)
                f.write(result)

def top_urls():
    print '\n*top_urls*\n\nThis method outputs the top 100 most common urls in a selection\n'
    print 'enter a valid file name:'
    user_in = raw_input('>> ')
    print 'please select a rumor (enter 0 for all):'
    i = 1
    for item in rumors:
        print '[%i] %s' % (i, item)
        i += 1
    user_select = raw_input('>> ')
    if user_select is 0:
        for x in rumors:
            _top_urls(db_name=default_database, fname=user_in, rumor=x)
    else:
        _top_urls(db_name=default_database, fname=user_in, rumor=rumors[int(user_select) - 1])
    print "done!\nexiting..."

def _text_by_time(db_name,rumor,fname,start_time,end_time,code):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_%s.csv" % (rumor.replace('/','_'), fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('time,rumor text\n')

    raw_data = db.m_connections[db_name].find({
        "created_ts":{
            "$gte":start_time,
            "$lte":end_time
        },
        "codes.rumor":rumor,
        "codes.code":code
    })

    for i,x in enumerate(raw_data):
        for y in x['codes']:
            if y['rumor'] == rumor:
                result = '"%s","%s","%s"\n' % (x['created_at'],
                                               x['user']['screen_name'],
                                               x['text'])
                try:
                    f.write(result.encode('utf-8'))
                except:
                    result = '"%s","%s"\n' % (x['created_at'],
                                              'unicode error')
                    f.write(result)
                print i,result

def text_by_time():
    print 'enter a valid file name:'
    fname_in = raw_input('>> ')
    print 'enter a rumor: (girl running, sunil, craft/seals, cell phone, proposal, jfk)'
    rumor_in = raw_input('>> ')
    print 'enter a code (misinfo, correction, speculation, hedge, question, other/unclear/neutral):'
    code_in = raw_input('>> ')

    print 'enter a start day (15 through 22):'
    day = int(raw_input('>> '))
    print 'enter a start hour (0 through 23):'
    hour = int(raw_input('>> '))
    print 'enter a start minute (0 through 59):'
    minute = int(raw_input('>> '))
    dateStart = datetime(2013,04,day,hour,minute)

    print 'enter an end day (15 through 22):'
    day = int(raw_input('>> '))
    print 'enter an end hour (0 through 23):'
    hour = int(raw_input('>> '))
    print 'enter an end minute (0 through 59):'
    minute = int(raw_input('>> '))
    dateEnd = datetime(2013,04,day,hour,minute,59)

    _text_by_time(db_name='new_boston',
                  rumor=rumor_in,
                  fname=fname_in,
                  start_time=dateStart,
                  end_time=dateEnd,
                  code=code_in)

def _gps_over_time(db_name, fname, rumor="all"):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_%s.csv" % (rumor.replace('/','_'), fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    print "querying database..."
    if rumor is "all":
        raw_data = db.m_connections[db_name].find({
        'place.coordinates.type':
            'Point'
        })
    else:
        raw_data = db.m_connections[db_name].find({
        'place.coordinates.type':
            'Point',
            "codes.rumor":rumor
        })
    f.write('title,url,text,author,time,code,lat,lon\n')
    print "writing data"
    for (i,data) in enumerate(raw_data):
        try:
            title = data['entities']['urls'][0]['title']
        except:
            title = ""
        try:
            url = data['entities']['urls'][0]['long-url']
        except:
            url = ""
        try:
            code = data['codes'][0]['code']
        except:
            code = ""
        text = data['text']
        author = data['user']['id']
        time = data['created_at']
        try:
            lat = data['place']['coordinates']['coordinates'][0]
        except:
            lat = ""
        try:
            lon = data['place']['coordinates']['coordinates'][1]
        except:
            lon = ""
        # print i ...for testing
        result = '"%s","%s","%s","%s","%s","%s","%s","%s"\n' % (title,url,text,author,time,code,lat,lon)
        # print result ... for testing
        try:
            f.write(result)
        except:
            f.write('decode error!\n')

def gps_over_time():
    print '\n*gps_over_time*\n\nThis method outputs features for all tweets with gps coordinates in a selection\n'
    print 'enter a valid file name:'
    user_in = raw_input('>> ')
    print 'please select a rumor (enter 0 for all):'
    i = 1
    for item in rumors:
        print '[%i] %s' % (i, item)
        i += 1
    user_select = raw_input('>> ')
    if int(user_select) is 0:
        _gps_over_time(db_name=default_database, fname=user_in)
    else:
        _gps_over_time(db_name=default_database, fname=user_in, rumor=rumors[int(user_select) - 1])
    print "done!\nexiting..."

if __name__ == "__main__":
    print "please use main.py to access this program"
