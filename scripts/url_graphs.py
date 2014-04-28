from connection import dbConnection
from collections import Counter
from datetime import datetime
import utils,re

def _url_by_domain_counter(db_name,rumor,top=100,write=True,url_type='domain'):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_top_%s.csv" % (rumor.replace('/','_').replace(' ','_'),url_type)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('domain,total,misinfo hits,correction hits\n')

    count = Counter()
    raw_data = db.m_connections[db_name].find({
        "counts.urls":{
            "$gt":0
        },
        'codes.rumor':rumor
    })

    for data in raw_data:
        url = [j[url_type] for j in data['entities']['urls'] if url_type in j]
        count.update(url)

    if write == True:
        for x in count.most_common(top):
            s1 = 'entities.urls.' + url_type
            query = {
                s1:x[0],
                'codes.rumor':rumor
            }
            info_type = Counter()
            new_data = db.m_connections[db_name].find(query)
            for y in new_data:
                info_type.update([y['codes'][0]['code']])
            misinfo = info_type['misinfo'] + info_type['speculation'] + info_type['hedge']
            correction = info_type['correction'] + info_type['question']
            result = '"%s","%s","%s","%s"\n' % (x[0],x[1],misinfo,correction)
            f.write(result.encode('utf-8'))

    else:
        result = []
        for x in count.most_common(top):
            result.append(x[0])

        return result

def _domain_over_time(db_name,rumor,domain,url_type):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    result = {}
    for i in range(15,23):      #15-23 (day)
        for j in range(0,24):   #0-24 (hour)
            for k in range(0,60,60):
                dateStart = datetime(2013,04,i,j,k)
                dateEnd = datetime(2013,04,i,j,(k+59),59)
                s1 = 'entities.urls.'+url_type
                query = {
                    "created_ts":{
                        "$gte":dateStart,
                        "$lte":dateEnd
                    },
                    s1:domain,
                    'codes.rumor':rumor
                }
                #print "time: %s,%s" % (dateStart,dateEnd)

                raw_data = db.m_connections[db_name].find(query).count()
                result[dateStart] = raw_data

    return result

def _unique_authors_per_domain(db_name,rumor,domain):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    query = {
        'entities.urls.domain':domain,
        'codes.rumor':rumor
    }
    raw_data = db.m_connections[db_name].find(query)

    count = Counter()
    reg_tag = re.compile('RT @', re.IGNORECASE)
    for x in raw_data:
        if re.match(reg_tag,x['text']) != None:
            count.update(['RT'])
        else:
            count.update(['No RT'])

    return count

def url_by_domain_counter(url_type):
    rumors = ['girl running','sunil','seals/craft','cell phone','proposal','jfk']
    for x in rumors:
        _url_by_domain_counter(db_name='new_boston',rumor=x,top=100,write=True,url_type=url_type)


# make sure to set BOTH date ranges properly
def domain_over_time(top,url_type):
    rumors = ['proposal']#['girl running','sunil','seals/craft','cell phone','proposal','jfk']
    for x in rumors:
        top_domains = _url_by_domain_counter(db_name='new_boston',rumor=x,top=top,write=False,url_type=url_type)
        result = {}
        for y in top_domains:
            result[y] = _domain_over_time(db_name='new_boston',rumor=x,domain=y,url_type=url_type)
        print ' [x] finished query'

        title = "%s_%s_over_time.csv" % (x.replace('/','_').replace(' ','_'),url_type)
        fpath = utils.write_to_data(path=title)
        f = open(fpath, 'w')
        f.write('"time",')
        for y in top_domains:
            f.write('"%s",' % y)
        f.write('\n')
        for i in range(15,23):      #15-23 (day)
            for j in range(0,24):   #0-24 (hour)
                for k in range(0,60,60):
                    dateStart = datetime(2013,04,i,j,k)
                    dateEnd = datetime(2013,04,i,j,(k+59),59)
                    f.write('%s,' % dateStart)
                    for y in top_domains:
                        out = '%i,' % result[y][dateStart]
                        f.write(out)
                    f.write('\n')

def unique_authors_per_domain(top):
    rumors = ['proposal']#['girl running','sunil','seals/craft','cell phone','proposal','jfk']
    for x in rumors:
        top_domains = _url_by_domain_counter(db_name='new_boston',rumor=x,top=top,write=False,url_type='domain')
        result = {}
        for y in top_domains:
            result[y] = _unique_authors_per_domain(db_name='new_boston',rumor=x,domain=y)
        print ' [x] finished query'

        title = "%s_domain_unique_authors.csv" % (x.replace('/','_').replace(' ','_'))
        fpath = utils.write_to_data(path=title)
        f = open(fpath, 'w')
        f.write('"domain","RT","No RT"\n')

        for y in top_domains:
            out = '"%s","%s","%s"\n' % (y,result[y]['RT'],result[y]['No RT'])
            f.write(out.encode('utf-8'))

if __name__ == "__main__":
    #domain_over_time(top=20,url_type='long-url')
    #url_by_domain_counter(url_type='domain')
    unique_authors_per_domain(top=10)
