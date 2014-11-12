from connection import dbConnection
from collections import Counter
import utils,random,re,nltk,os,csv

def _create_random_sample(rumors,num,db_name,scrub_url=True):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=db_name)

    print 'enter a valid file name:'
    fname_in = raw_input('>> ')
    title = "%s.csv" % (fname_in)
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('"rumor","id","scrubbed text","text"\n')

    for x in rumors:
        query1 = db.m_connections[db_name[0]].find({'codes.rumor':x,
                                                    'intersect':0})
        tweets = [long(z['id']) for z in query1]
        result = []
        count = 0
        while count < num:
            old_tweet = random.choice(tweets)
            new_tweet = db.m_connections[db_name[1]].find_one({'id':old_tweet})
            text = new_tweet['text']
            temp = None
            while text is not temp:
                temp = text
                text = re.sub('RT .*?:','',text).strip()
                text = re.sub('@ .*?:','',text).strip()
                if scrub_url is True:
                    text = re.sub('http.*?\s|http.*?$','',text).strip()
            unique = True
            for y in result:
                if nltk.metrics.edit_distance(text,y) < 20:
                    unique = False
            if unique is True:
                result.append(text)
                out = '"%s","%s","%s",\n' % (x,
                                                 new_tweet['id'],
                                                 new_tweet['text'].replace('"',''))
                f.write(out.encode('utf-8'))
                count += 1

def agregate_results(file_in):
    # *** INPUT ***
    # read in all files in user specified sub-directory and open output file
    # potentially change this to absolute directory path
    script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
    print 'enter name of sub-directory'
    sub_dir_in = raw_input('>> ')
    abs_file_path = os.path.join(script_dir, sub_dir_in)
    overall_out = open('overall_out.csv','w')
    disagreement_out = open('disagreement_out.csv','w')
    tweets = {}

    # open and iterate through all files in directory
    for fname in os.listdir(abs_file_path):
        fname2 = os.path.join(abs_file_path,fname)
        if os.path.isfile(fname2):
            with open(fname2, 'rb') as f:
                # get all header column names
                reader = csv.reader(f)
                header = reader.next()
                for row in reader:
                    # key: tweet id
                    # record rumor and text
                    if row[1] not in tweets:
                        tweets[row[1]] = {'rumor':row[0],'text':row[2]}
                        #create a counter for each column to record 'x' or ''
                        for index in xrange(3,len(header)):
                            tweets[row[1]][header[index]] = Counter()
                    # count weather each cell in a row contains an 'x' or ''
                    for index in xrange(3,len(header)):
                        tweets[row[1]][header[index]].update([row[index]])

    # *** OUTPUT ***
    code_dis_counter = Counter()
    rumor_dis_counter = Counter()
    for tweet in tweets:
        disagree = False
        result = '%s,%s,"%s"' % (tweet,tweets[tweet]['rumor'],tweets[tweet]['text'])
        dis_result = result
        for code in tweets[tweet]:
            if code is not 'rumor' and code is not 'text':
                if tweets[tweet][code]['x'] is not 0:
                    # if the code has been applied by any coder, record it
                    result += ',%s' % code
                    # if there's disagreement, mark the number of coders who
                    # applied the code
                    if len(tweets[tweet][code].most_common()) > 1:
                        result += '(%s)' %str(tweets[tweet][code]['x'])
                        agreement =  float(tweets[tweet][code].most_common()[0][1]) / (tweets[tweet][code].most_common()[1][1] + tweets[tweet][code].most_common()[0][1])
                        if agreement <= .75:
                            disagree = True
                            dis_result += ',%s,%s' % (code,agreement)
                        code_dis_counter.update([code])
                        rumor_dis_counter.update([tweets[tweet]['rumor']])
        if disagree:
            dis_result += '\n'
            disagreement_out.write(dis_result)
        result += '\n'
        overall_out.write(result)
    print code_dis_counter
    print rumor_dis_counter

def main():
    rumors = ['girl running','jfk','sunil','seals/craft','cell phone','proposal']
    _create_random_sample(rumors=rumors,num=100,db_name=['new_boston',
                                                         'gnip_boston'])
    #agregate_results(file_in='')

if __name__ == "__main__":
    main()
