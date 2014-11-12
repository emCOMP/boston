from connection import dbConnection
from collections import Counter
from datetime import datetime
import utils

def _intersection_volumes(dbs):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=dbs)

    title = "total_overlap.csv"
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')

def deleted_accounts(db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "top_deletion_accounts_(no_rumor).csv"
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')
    query = {'intersect':1}
    tweets = db.m_connections[db_name].find(query)
    count = Counter()
    for x in tweets:
        count.update([x['user']['screen_name']])
    for x in count.most_common(1000):
        result = '%s,%s\n' % (x[0],x[1])
        f.write(result)

def main():
    deleted_accounts(db_name="new_boston")

if __name__ == "__main__":
    main()
