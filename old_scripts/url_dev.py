from pymongo import MongoClient
from datetime import datetime
import collections,re,random

client = MongoClient('z')
db = client.new_boston
collection = db.tweets
tweets = db.tweets

main_collection = db.tweets
main_tweets = db.tweets

def counts_entities_match():
    count = 0
    for x in range(7):
        query = 'entities.urls.%i' % (x)
        data = tweets.find({'counts.urls':(x+1),query:{'$exists':'true'}}).count()
        count += data
    print count

def url_regex_count():
    uncounted = collections.Counter()
    uncaught = collections.Counter()
    good = collections.Counter()
    total = collections.Counter()
    f = open('data/url_matches.csv','w')
    for x in range(7):
        reg_tag = re.compile('http', re.IGNORECASE)
        data = tweets.find({'counts.urls':(x),
                            'text':reg_tag,
                            'codes.rumor':{'$exists':'true'}})
        for i,y in enumerate(data):
            urls = y['text'].count('http')
            if urls > y['counts']['urls']:
                for z in y['codes']:
                    uncounted.update([z['rumor']])
                    total.update([z['rumor']])
                try:
                    f.write('"%s","%s"\n' % (y['user']['id'],y['text']))
                except:
                    f.write('"%s","%s"\n' % (y['user']['id'],'decode error'))
            elif urls < y['counts']['urls']:
                for z in y['codes']:
                    uncaught.update([z['rumor']])
                    total.update([z['rumor']])
            else:
                for z in y['codes']:
                    good.update([z['rumor']])
                    total.update([z['rumor']])

    f2 = open('data/url_stats.txt','w')
    print 'uncounted urls: %s' % uncounted
    f2.write('uncounted urls: "%s"\n' % uncounted)
    print 'uncaught urls: %s' % uncaught
    f2.write('uncaught urls: "%s"\n' % uncaught)
    print 'matched urls: %s' % good
    f2.write('good urls: "%s"\n' % good)
    print 'total urls: %s' % total
    f2.write('total urls: "%s"\n' % total)

def unique_urls():
    url_count = collections.Counter()
    title_count = collections.Counter()
    domain = tweets.distinct('entities.urls.domain')
    data = tweets.find({'counts.urls':{'$gt':0}})
    #title = tweets.distinct('entities.urls.title')
    print 'domain: ' + str(len(domain))
    for x in data:
        for y in x['entities']['urls']:
            try:
                url_count.update([y['long-url']])
            except:
                print 'no url'
            try:
                title_count.update([y['title']])
            except:
                print 'no title'
    print len(title_count)
    print len(domain)
    #print 'url: ' + str(len(url))
    #print 'title: ' + str(len(title))

def url_repair():
    # set up regex for finding links, counter, logging.  use http for now
    find_link = re.compile('http.*?\s|http.*?$', re.IGNORECASE)
    f = open('data/url_matches_jfk.csv','w')
    f.write('repaired tweet id,reference tweet id\n')
    count = collections.Counter()
    bad_links = []

    # get all tweets with any number of known urls (1 through 6)
    # if the count < number of http matches, add to "bad_urls"
    # create a random sampling of 5000 tweets with bad_urls
    for x in range(7):
        data = tweets.find({'counts.urls':(x)})
        #data = tweets.find({'user.id':'4863301'})
        for y in data:
            urls = y['text'].count('http')
            if urls > y['counts']['urls']:
                bad_links.append(y)

    # CODE FOR RANDOM SAMPLE
    #indexes = random.sample(xrange(0,len(bad_links)-1),10)
    #sample = [bad_links[x] for x in indexes]
    #count.update({'bad links':len(bad_links),'sample size':len(sample)})

    # CODE FOR COMPLETE SET
    count.update({'bad links':len(bad_links)})
    sample = bad_links

    for i,y in enumerate(sample):
        print (float(i)/len(sample))
        links = find_link.findall(y['text'])
        links = [z.strip('\n').strip(' ').strip('...').strip(u'2026') for z in links]
        known_links = [z['expanded_url'] for z in y['entities']['urls']]
        result = [z for z in links if z not in known_links]
        if len(links) - len(known_links) == len(result):
            #print 'text: %s' % y['text']
            #print links
            new_data = tweets.find({'text':(re.compile(re.escape(y['text']))),
                                    'counts.urls':len(links)}).sort('created_ts',1).limit(1)
            try:
                new_data = new_data.next()
                #print 'matches: %s' % new_data['text']
                #print len(links)
            except:
                #print 'matches: %s' % None
                new_data = None
                text = y['text']
                links_strip = [z for z in links]
                start_text = None
                while new_data == None and start_text is not text:
                    start_text = text
                    text = text.rstrip('...').strip()
                    #if len(links_strip) > 0:
                    #    text = text.rstrip(links_strip[-1]).strip().strip('...')
                    #    links_strip.pop()
                    text = re.sub('RT .*?:','',text).strip()
                    text = re.sub('http.*?\s|http.*?$','',text).strip()
                    #print text,len(links)
                    new_data = tweets.find({'text':(re.compile(re.escape(text))),
                                            'counts.urls':len(links)}).sort('created_ts',1).limit(1)
                    try:
                        new_data = new_data.next()
                    except:
                        new_data = None
                        #print 'matches: %s, %s' % (new_data, text)
            if new_data == None:
                count.update(['unmatched'])
            else:
                count.update(['matched'])
                f.write('%s,%s\n' % (y['user']['id'],new_data['user']['id']))
                for z in new_data['entities']['urls']:
                    if z['expanded_url'] not in known_links:
                        z['reconstructed'] = True
                        #print z
                        #main_tweets.update({'user.id':y['user']['id']},
                        #                   {'$push':{'entities.urls':z}})
                        main_tweets.update({'user.id':y['user']['id']},
                                           {'$push':{'entities.urls':z}})
                        main_tweets.update({'user.id':y['user']['id']},
                                           {'$inc':{'counts.urls':1}})
        else:
            count.update(['uncaught'])
    print count
    f.write('%s' % count)

def long_url_repair():
    # set up counter, logging.
    f = open('data/long_url_matches_test.csv','w')
    f.write('repaired tweet id,reference tweet id\n')
    f2 = open('data/long_url_error_log.csv','w')
    count = collections.Counter()
    bad_links = []

    # get all tweets with any number of known urls
    # if the long-url = expanded_url, add to "bad_urls"
    # expansion errors recorded in data/log_url_error_log
    # data = tweets.find({'counts.urls':{'$gte':1}}).limit(10000)
    data = tweets.find({'id':'324976766118662144'})
    for x in data:
        print data
        for y in x['entities']['urls']:
            try:
                if y['long-url'] == y['expanded_url']:
                    bad_links.append(x)
            except:
                f2.write('%s\n' % x['id'])

    count.update({'bad links':len(bad_links)})
    sample = [bad_links[0]]
    print 'done'

    # iterate through bad links, mentions and urls
    # find a document where the long-url does not contain the expanded_url text
    # if there are multiple, pick the first by time
    for i,y in enumerate(sample):
        print y
        text = y['text']
        start_text = ''
        while start_text is not text:
            start_text = text
            text = text.rstrip('...').strip()
            text = re.sub('RT .*?:','',text).strip()
            text = re.sub('http.*?\s|http.*?$','',text).strip()
        print text
        tweet_text = re.compile(re.escape(text))
        for z in y['entities']['urls']:
            if z['long-url'] == z['expanded_url']:
                link_text = z['expanded_url']
                reg_link_text = re.compile(z['expanded_url'])
                print link_text,reg_link_text
        new_data = tweets.find({'text':tweet_text,
                                'entities.urls.expanded_url':reg_link_text,
                                'entities.urls.long-url':{'$not':reg_link_text}}).sort('created_ts',1).limit(1)

        try:
            new_data = new_data.next()
            print "sucess"
        except:
            new_data = None
            print "fail"
        print 'done'
        # if there are no matches, add to unmatched
        # else find matching document in db and update url entry
        if new_data == None:
            count.update(['unmatched'])
        else:
            count.update(['matched'])
            f.write('%s,%s\n' % (y['user']['id'],new_data['user']['id']))
            for z in new_data['entities']['urls']:
                if reg_link_text.search(z['expanded_url']) != None:
                    z['reconstructed'] = True
                    #print z
                    #main_tweets.update({'user.id':y['user']['id']},
                    #                   {'$push':{'entities.urls':z}})
                    main_tweets.update({'user.id':y['user']['id'],
                                        'entities.urls.expanded_url':reg_link_text},
                                       {'$set':{'entities.urls.$':z}})

    print count
    f.write('%s' % count)

def test():
    f = open('data/long_url_matches_test.csv','w')
    f.write('repaired tweet id,reference tweet id\n')
    f2 = open('data/long_url_error_log.csv','w')
    count = collections.Counter()
    bad_links = []

    # get all tweets with any number of known urls
    # if the long-url = expanded_url, add to "bad_urls"
    # expansion errors recorded in data/log_url_error_log
    data = tweets.find({'counts.urls':{'$gte':1}}).limit(10000)
    for x in data:
        for y in x['entities']['urls']:
            try:
                if y['long-url'] == y['expanded_url']:
                    bad_links.append(x)
            except:
                f2.write('%s\n' % x['_id'])

    count.update({'bad links':len(bad_links)})
    print bad_links[0]

def count_RT():
    num_RT = collections.Counter()
    reg1 = re.compile('rt @',re.IGNORECASE)
    reg2 = re.compile('r @.*?\s',re.IGNORECASE)
    reg3 = re.compile('via @.*?\s',re.IGNORECASE)
    reg4 = re.compile('/"@.*?\s',re.IGNORECASE)
    data = db.tweets.find({'codes.rumor':{'$exists':'true'},'text':reg1})
    for x in data:
        for y in x['codes']:
            num_RT.update([y['rumor']])

    print num_RT

if __name__ == "__main__":
    long_url_repair()
