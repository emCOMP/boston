from connection import dbConnection
from collections import Counter
from datetime import datetime
import utils

def url_by_domain_counter(db_name,rumor):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_top_domains.csv" % rumor.replace('/','_').replace(' ','_')
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
        url = [j['domain'] for j in data['entities']['urls'] if 'domain' in j]
        count.update(url)

    for x in count.most_common(100):
        info_type = Counter()
        new_data = db.m_connections[db_name].find({
            'entities.urls.domain':x[0],
            'codes.rumor':rumor
        })
        for y in new_data:
            info_type.update([y['codes'][0]['code']])
        misinfo = info_type['misinfo'] + info_type['speculation'] + info_type['hedge']
        correction = info_type['correction'] + info_type['question']
        result = '"%s","%s","%s","%s"\n' % (x[0],x[1],misinfo,correction)
        try:
            f.write(result)
        except:
            f.write('decode error!\n')

def main():
    rumors = ['girl running','sunil','seals/craft','cell phone','proposal','jfk']
    for x in rumors:
        url_by_domain_counter(db_name='new_boston',rumor=x

if) __name__ == "__main__":
    main()
