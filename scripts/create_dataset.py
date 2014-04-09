import utils, re
from connection import dbConnection


def _dataset_from_rumor(rumor,dbname):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_tweets.csv" % (rumor.replace('/','_'), fname)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('tweet_ID,text,author,time,retweeted,original_iD\n')

    if rumor == 'girl running':
        terms = re.compile('girl ', re.IGNORECASE)
        terms2 = re.compile('running', re.IGNORECASE)
        raw_data = db.m_connections[db_name].find({
            '$and'[
                {'text':terms},
                {'text':terms2}
            ]
        },{
            # enter fields to be printed
        })
