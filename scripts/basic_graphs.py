from connection import dbConnection
from collections import Counter
from datetime import datetime
import utils

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

                raw_data = db.m_connections[db_name].find({
                    "created_ts":{
                        "$gte":dateStart,
                        "$lte":dateEnd
                    },
                    "codes.rumor":rumor
                },{
                    "codes.code":1
                })
                result = ''
                for x in raw_data:
                    count.update([x['codes'][0]['code']])

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

                f.write(result)

def _text_by_time(db_name,rumor,fname,start_time,end_time,code):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_%s.csv" % (rumor.replace('/','_'), fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('time,rumor text\n')

    raw_data = db.m_connections[db_name].find({
        "created_ts":{
            "$gte":dateStart,
            "$lte":dateEnd
        },
        "codes.rumor":rumor,
        "codes.code":code
    })

    for x in raw_data:
        result = '"%s","%s\n"' % (x['created_at'],x['text'])
        f.write(result)

def text_by_time():
    print 'enter a valid file name:'
    fname_in = raw_input('>> ')
    print 'enter a rumor:'
    rumor_in = raw_input('>> ')
    print 'enter a code:'
    code_in = raw_input('>> ')
    print 'enter a day (15 through 22):'
    day = int(raw_input('>> '))
    print 'enter an hour (0 through 23):'
    minute = int(raw_input('>> '))
    print 'enter a minute (0 through 58):'
    second = raw_input('>> ')
    dateStart = datetime(2013,04,day,hour,minute)
    dateEnd = datetime(2013,04,day,hour,minute,59)

    _text_by_time(db_name='new_boston',
                  rumor=rumor_in,
                  fname=fname_in,
                  start_time=dateStart,
                  end_time=dateEnd,
                  code=code_in)

def rumor_over_time():
    rumors = ['girl running','sunil','seals/craft','cell phone','proposal','jfk']

    print 'enter a valid file name:'
    user_in = raw_input('>> ')

    for x in rumors:
        _rumor_over_time(db_name='new_boston',rumor=x,gran=True,fname=user_in)

if __name__ == "__main__":
    rumor_over_time()
