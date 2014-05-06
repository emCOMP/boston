import os
from collections import Counter

def write_to_data(path=''):
    result = os.path.join(os.path.dirname(__file__),os.pardir,'data/') + path
    return result

def counter_data(count,gran=False):
    codes = {}

    if gran:
        codes['misinfo'] = count['misinfo']
        codes['speculation'] = count['speculation']
        codes['hedge'] = count['hedge']
        codes['correction'] = count['correction']
        codes['question'] = count['question']
        codes['other'] = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']

    else:
        codes['misinfo'] = count['misinfo'] + count['speculation'] + count['hedge']
        codes['correction'] = count['correction'] + count['question']
        codes['other'] = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']

    return codes

# not working - needs dateTime fix
def time_series(query,db):
    result = {}

    for i in range(15,23):      #15-23 (day)
        for j in range(0,24):   #0-24 (hour)
            for k in range(0,60,10):
                dateStart = datetime(2013,04,i,j,k)
                dateEnd = datetime(2013,04,i,j,(k+9),59)
                #print "time: %s,%s" % (dateStart,dateEnd)

                raw_data = db.m_connections[db_name].find(query).count()
                result[datestart] = raw_data

    return result
