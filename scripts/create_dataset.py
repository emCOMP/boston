import utils, re
from connection import dbConnection


def _dataset_from_rumor(rumor,db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_tweets.csv" % (rumor.replace('/','_'))
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('tweet_ID,text,author,time,retweeted,original_iD\n')
    terms = []

    if rumor == 'girl running':
        terms.append(re.compile('girl ', re.IGNORECASE))
        terms.append(re.compile('running', re.IGNORECASE))
        raw_data = db.m_connections[db_name].find({
            '$and':[
                {'text':terms[0]},
                {'text':terms[1]}
            ]
        })
    elif rumor == 'craft/seals':
        terms.append(re.compile('blackwater', re.IGNORECASE))
        terms.append(re.compile('craft', re.IGNORECASE))
        terms.append(re.compile('security', re.IGNORECASE))
        terms.append(re.compile('navy seal', re.IGNORECASE))
        terms.append(re.compile('black ops', re.IGNORECASE))
        raw_data = db.m_connections[db_name].find({
            '$or':[
                {'text':terms[0]},
                {'$and':[{'text':terms[1]},{'text':terms[2]}]},
                {'text':terms[3]},
                {'text':terms[4]}
            ]
        })
    elif rumor == 'sunil':
        terms.append(re.compile('sunil', re.IGNORECASE))
        terms.append(re.compile('tripathi', re.IGNORECASE))
        raw_data = db.m_connections[db_name].find({
            '$or':[
                {'text':terms[0]},
                {'text':terms[1]}
            ]
        })
    elif rumor == 'cell phone':
        terms.append(re.compile('cell ', re.IGNORECASE))
        terms.append(re.compile('mobile', re.IGNORECASE))
        terms.append(re.compile('phone', re.IGNORECASE))
        terms.append(re.compile('wireless', re.IGNORECASE))
        terms.append(re.compile('service', re.IGNORECASE))
        terms.append(re.compile('network', re.IGNORECASE))
        terms.append(re.compile('call our from this site', re.IGNORECASE))
        raw_data = db.m_connections[db_name].find({
            '$and':[
                {'$or':[
                    {'text':terms[0]},
                    {'text':terms[1]},
                    {'text':terms[2]},
                    {'text':terms[3]}
                ]},
                {'$or':[
                    {'text':terms[4]},
                    {'text':terms[5]}
                ]},
                {'text':{'$not':terms[6]}}
            ]
        })
    elif rumor == 'proposal':
        terms.append(re.compile('propos', re.IGNORECASE))
        terms.append(re.compile('marry', re.IGNORECASE))
        terms.append(re.compile('girl', re.IGNORECASE))
        terms.append(re.compile('woman', re.IGNORECASE))
        raw_data = db.m_connections[db_name].find({
            '$and':[
                {'$or':[
                    {'text':terms[0]},
                    {'text':terms[1]},
                ]},
                {'$or':[
                    {'text':terms[2]},
                    {'text':terms[3]}
                ]},
            ]
        })
    elif rumor == 'jfk':
        terms.append(re.compile('jfk', re.IGNORECASE))
        terms.append(re.compile('bomb', re.IGNORECASE))
        terms.append(re.compile('library', re.IGNORECASE))
        raw_data = db.m_connections[db_name].find({
            '$or':[
                    {'text':terms[0]},
                {'$and':[
                    {'text':terms[1]},
                    {'text':terms[2]}
                ]}
            ]
        })

    for x in raw_data:
        if 'retweeted_status' in x:
            retweeted = 1
            original = x['retweeted_status']['id']
        else:
            retweeted = 0
            original = 0

        result = '"%s","%s","%s","%s","%s","%s"\n' % (x['id'],
                                                      x['text'],
                                                      x['user']['screen_name'],
                                                      x['created_ts'],
                                                      retweeted,
                                                      original)
        f.write(result.encode('utf-8'))

def dataset_from_rumor():
    rumors = ['girl running','sunil','craft/seals','cell phone','proposal','jfk']
    for x in rumors:
        _dataset_from_rumor(rumor=x,db_name='gnip_boston')

if __name__ == "__main__":
    dataset_from_rumor()
