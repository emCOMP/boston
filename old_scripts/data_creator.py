from pymongo import MongoClient
from datetime import datetime
#import counter,re

client = MongoClient()
db = client.boston
collection = db.tweets
tweets = db.tweets

def url_counter():
	count = counter.Counter()
	raw_data = tweets.find({"counts.urls":{"$gt":0}},{"entities.urls":1})
	print 'order by:\n[0] url\n[1] domain\n[2] title\n'
	user_in = raw_input('>> ')
	if user_in == '0':
		f = open('data/top_urls.csv', 'w')
		f.write('url,count\n')
	elif user_in == '1':
		f = open('data/top_domains.csv', 'w')
		f.write('domain,count\n')
	elif user_in == '2':
		f = open('data/top_url_titles.csv', 'w')
		f.write('url landing title,count\n')
	for (i,data) in enumerate(raw_data):
		if user_in == '0':
			url = [j['long-url'] for j in data['entities']['urls'] if 'long-url' in j]
		elif user_in == '1':
			url = [j['domain'] for j in data['entities']['urls'] if 'domain' in j]
		elif user_in == '2':
			url = [j['title'] for j in data['entities']['urls'] if 'title' in j]
		count.update(url)
		print 'row: %d' % i

	for x in count.most_common(1000):
		result = '"%s","%s"\n' % (x[0],x[1])
		try:
			print result
			f.write(result)
		except:
			f.write('decode error!\n')

def top_urls_by_domain():
	count = counter.Counter()
	print 'enter domain name:'
	user_in = raw_input('>> ')
	raw_data = tweets.find({"entities.urls.domain":user_in})
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	f.write('url landing title,count\n')
	for (i,data) in enumerate(raw_data):
		url = [j['title'] for j in data['entities']['urls'] if 'title' in j]
		count.update(url)
		print 'row: %d' % i

	for x in count.most_common(1000):
		result = '"%s","%s"\n' % (x[0],x[1])
		try:
			print result
			f.write(result)
		except:
			f.write('decode error!\n')


# TODO: change queries and datetimes
def key_word_counter_over_time():
	print 'search for:\n[0] hashtag\n[1] key word in text\n[2] url\n[3] domain\n[4] page title\n'
	option = raw_input('>> ')
	tag = raw_input('tag or key word: ')
	reg_tag = re.compile(tag, re.IGNORECASE)
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	f.write('time,"%s"\n' % tag)
	if option == '0':
		for i in range(15,23):		#15-23 (day)
			for j in range(0,24):	#0-24 (hour)
				dateStart = datetime(2013,04,i,j)
				dateEnd = datetime(2013,04,i,j,59,59)
				print "time: %s,%s" % (dateStart,dateEnd)
				print "tag: " + tag
				#print "query: select count(*) from tweets where (text like '%" + tag + "%' and time between '" + dateStart + "' and '" + dateEnd + "')"
				raw_data = tweets.find({"counts.hashtags":{"$gt":0},"created_ts":{"$gte":dateStart,"$lte":dateEnd},"entities.hashtags.text":tag}).count()
				print raw_data
				result = '"%s",%d\n' % (dateStart,raw_data)
				f.write(result)
	elif option == '1':
		for i in range(15,23):		#15-23 (day)
			for j in range(0,24):	#0-24 (hour)
				dateStart = datetime(2013,04,i,j)
				dateEnd = datetime(2013,04,i,j,59,59)
				print "time: %s,%s" % (dateStart,dateEnd)
				print "tag: " + tag
				#print "query: select count(*) from tweets where (text like '%" + tag + "%' and time between '" + dateStart + "' and '" + dateEnd + "')"
				raw_data = tweets.find({"created_ts":{"$gte":dateStart,"$lte":dateEnd},"text":reg_tag}).count()
				print raw_data
				result = '"%s",%d\n' % (dateStart,raw_data)
				f.write(result)
	elif option == '2':
		for i in range(15,23):		#15-23 (day)
			for j in range(0,24):	#0-24 (hour)
				dateStart = datetime(2013,04,i,j)
				dateEnd = datetime(2013,04,i,j,59,59)
				print "time: %s,%s" % (dateStart,dateEnd)
				print "tag: " + tag
				#print "query: select count(*) from tweets where (text like '%" + tag + "%' and time between '" + dateStart + "' and '" + dateEnd + "')"
				raw_data = tweets.find({"counts.urls":{"$gt":0},"created_ts":{"$gte":dateStart,"$lte":dateEnd},"entities.urls.long-url":tag}).count()
				print raw_data
				result = '"%s",%d\n' % (dateStart,raw_data)
				f.write(result)
	elif option == '3':
		for i in range(15,23):		#15-23 (day)
			for j in range(0,24):	#0-24 (hour)
				dateStart = datetime(2013,04,i,j)
				dateEnd = datetime(2013,04,i,j,59,59)
				print "time: %s,%s" % (dateStart,dateEnd)
				print "tag: " + tag
				#print "query: select count(*) from tweets where (text like '%" + tag + "%' and time between '" + dateStart + "' and '" + dateEnd + "')"
				raw_data = tweets.find({"counts.urls":{"$gt":0},"created_ts":{"$gte":dateStart,"$lte":dateEnd},"entities.urls.domain":tag}).count()
				print raw_data
				result = '"%s",%d\n' % (dateStart,raw_data)
				f.write(result)
	elif option == '4':
		for i in range(15,23):		#15-23 (day)
			for j in range(0,24):	#0-24 (hour)
				dateStart = datetime(2013,04,i,j)
				dateEnd = datetime(2013,04,i,j,59,59)
				print "time: %s,%s" % (dateStart,dateEnd)
				print "tag: " + tag
				#print "query: select count(*) from tweets where (text like '%" + tag + "%' and time between '" + dateStart + "' and '" + dateEnd + "')"
				raw_data = tweets.find({"counts.urls":{"$gt":0},"created_ts":{"$gte":dateStart,"$lte":dateEnd},"entities.urls.title":tag}).count()
				print raw_data
				result = '"%s",%d\n' % (dateStart,raw_data)
				f.write(result)

def tweet_aggregator():
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	raw_data = tweets.find({"entities.urls.domain":"www.reddit.com"})
	for (i,data) in enumerate(raw_data):
		title = [j['title'] for j in data['entities']['urls'] if ('title' in j and j['domain'] == "www.reddit.com")][0]
		url = [j['long-url'] for j in data['entities']['urls'] if ('long-url' in j and j['domain'] == "www.reddit.com")][0]
		text = data['text']
		author = data['user']['id']
		time = data['created_at']
		print i
		result = '"%s","%s","%s","%s","%s"\n' % (title,url,text,author,time)
		print result
		try:
			f.write(result)
		except:
			f.write('decode error!\n')

def other():
	tag = 'sunil'
	reg_tag = re.compile(tag, re.IGNORECASE)
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	f.write('title,url,text\n')
	raw_data = tweets.find({"entities.urls.domain":"www.reddit.com","text":reg_tag})
	for data in raw_data:
		title = [x['title'] for x in data['entities']['urls'] if x['domain'] == 'www.reddit.com']
		url = [x['long-url'] for x in data['entities']['urls'] if x['domain'] == 'www.reddit.com']
		result = '"%s","%s","%s"\n' % (title[0],url[0],data['text'])
		try:
			f.write(result)
		except:
			f.write('decode error!\n')

def rumors_over_time():
        tag = raw_input('rumor: ')
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	f.write('time,misinfo,correction,unrelated/neutral/other\n')
        for i in range(15,23):		#15-23 (day)
       		for j in range(0,24):	#0-24 (hour)
                        for k in range(0,60,10):
                                count = counter.Counter()
                                dateStart = datetime(2013,04,i,j,k)
                                dateEnd = datetime(2013,04,i,j,(k+9),59)
                                print "time: %s,%s" % (dateStart,dateEnd)
                                raw_data = tweets.find({"created_ts":{"$gte":dateStart,"$lte":dateEnd},"codes.rumor":tag},{"codes.code":1})
                                for x in raw_data:
                                        count.update([x['codes'][0]['code']])
                                misinfo = count['misinfo'] + count['speculation'] + count['hedge']
                                correction = count['correction'] + count['question']
                                other = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']
                                result = '"%s",%d,%d,%d\n' % (dateStart,misinfo,correction,other)
                                f.write(result)

def rumor_top_urls():
        tag = raw_input('rumor: ')
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	f.write('url,total,misinfo,correction,unrelated/neutral/other\n')
	url_counter = counter.Counter()
	raw_data = tweets.find({"counts.urls":{"$gt":0},"codes.rumor":tag},{"entities.urls":1})
	for (i,data) in enumerate(raw_data):
                url = [j['long-url'] for j in data['entities']['urls'] if 'long-url' in j]
		url_counter.update(url)
		print 'row: %d' % i

	for x in url_counter.most_common(50):
                misinfo = tweets.find({'entities.urls.long-url':str(x[0]),'codes.rumor':tag,'$or':[{'codes.code':'misinfo'},{'codes.code':'speculation'},{'codes.code':'hedge'}]}).count()
                correction = tweets.find({'entities.urls.long-url':str(x[0]),'codes.rumor':tag,'$or':[{'codes.code':'correction'},{'codes.code':'question'}]}).count()
                other = tweets.find({'entities.urls.long-url':str(x[0]),'codes.rumor':tag,'$or':[{'codes.code':'unrelated'},{'codes.code':'other/unclear/neutral'},{'codes.code':'unclear'},{'codes.code':''},{'codes.code':'discussion - justifying'},{'codes.code':'discussion - question'},{'codes.code':'other'},{'codes.code':'discussion'}]}).count()
		result = '"%s",%d,%d,%d,%d\n' % (x[0],x[1],misinfo,correction,other)
		try:
			print result
			f.write(result)
		except:
			print 'decode error!\n'

# enter grouping from original query to generate list by tag...
def rumor_top_urls_by_code():
        tag = raw_input('rumor: ')
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	f.write('url,total,misinfo,correction,unrelated/neutral/other\n')
	url_counter = counter.Counter()
	option = raw_input('by code: ')
        if option == 'misinfo':
                raw_data = tweets.find({"counts.urls":{"$gt":0},"codes.rumor":tag,'$or':[{'codes.code':'misinfo'},{'codes.code':'speculation'},{'codes.code':'hedge'}]},{"entities.urls":1})
        elif option == 'correction':
                raw_data = tweets.find({"counts.urls":{"$gt":0},"codes.rumor":tag,'$or':[{'codes.code':'correction'},{'codes.code':'question'}]})
        elif option == 'other':
                raw_data = tweets.find({"counts.urls":{"$gt":0},"codes.rumor":tag,'$or':[{'codes.code':'unrelated'},{'codes.code':'other/unclear/neutral'},{'codes.code':'unclear'},{'codes.code':''},{'codes.code':'discussion - justifying'},{'codes.code':'discussion - question'},{'codes.code':'other'},{'codes.code':'discussion'}]})
	for (i,data) in enumerate(raw_data):
                url = [j['long-url'] for j in data['entities']['urls'] if 'long-url' in j]
		url_counter.update(url)
		print 'row: %d' % i

	for x in url_counter.most_common(50):
                misinfo = tweets.find({'entities.urls.long-url':str(x[0]),'codes.rumor':tag,'$or':[{'codes.code':'misinfo'},{'codes.code':'speculation'},{'codes.code':'hedge'}]}).count()
                correction = tweets.find({'entities.urls.long-url':str(x[0]),'codes.rumor':tag,'$or':[{'codes.code':'correction'},{'codes.code':'question'}]}).count()
                other = tweets.find({'entities.urls.long-url':str(x[0]),'codes.rumor':tag,'$or':[{'codes.code':'unrelated'},{'codes.code':'other/unclear/neutral'},{'codes.code':'unclear'},{'codes.code':''},{'codes.code':'discussion - justifying'},{'codes.code':'discussion - question'},{'codes.code':'other'},{'codes.code':'discussion'}]}).count()
		result = '"%s",%d,%d,%d,%d\n' % (x[0],(misinfo+correction+other),misinfo,correction,other)
		try:
			print result
			f.write(result)
		except:
			print 'decode error!\n'

def rumor_urls_over_time():
        tag = raw_input('rumor: ')
	filename = raw_input('file name (be careful not to overwrite): ')
	title = "data/%s.csv" % filename
	f = open(title, 'w')	#create new csv
	f.write('url,total,misinfo,correction,unrelated/neutral/other\n')
	url_counter = counter.Counter()
	raw_data = tweets.find({"counts.urls":{"$gt":0},"codes.rumor":tag},{"entities.urls":1})
	for (i,data) in enumerate(raw_data):
                url = [j['long-url'] for j in data['entities']['urls'] if 'long-url' in j]
		url_counter.update(url)
		print 'row: %d' % i
        f.write('time,')
        for k in url_counter.most_common(10):
                f.write('"%s",' % k[0])
        f.write('\n')
	for i in range(15,23):		#15-23 (day)
       		for j in range(0,24):	#0-24 (hour)
                        dateStart = datetime(2013,04,i,j)
                        dateEnd = datetime(2013,04,i,j,59,59)
                        print "time: %s,%s" % (dateStart,dateEnd)
                        f.write('%s,' % dateStart)
                        for k in url_counter.most_common(10):
                                f.write('%d,' % (tweets.find({"created_ts":{"$gte":dateStart,"$lte":dateEnd},"entities.urls.long-url":k[0],"codes.rumor":tag}).count()))
                        f.write('\n')

def single_node_network():
	tag = raw_input('tag: ')
        node = re.compile(tag, re.IGNORECASE)
	title = "data/%s_node_network.csv" % tag
	f = open(title, 'w')	#create new csv
	raw_data = tweets.find({'entities.hashtags.text':node}).count()
        f.write("NODE\n")
	f.write('"%s",%d\n' % (tag,raw_data))

	f.write('EDGES\n')
	edge_counter = counter.Counter()	#add count(100) with function to get only counts of existing hashtags
        raw_data = tweets.find({'entities.hashtags.text':node})
	for (i,row) in enumerate(raw_data):
                print i,row['entities']['hashtags']
		result = [x['text'].lower() for x in row['entities']['hashtags']]
		edge_counter.update(result)

	for y in edge_counter.most_common(100):
		result = '"%s","%s","%s"\n' % (tag,y[0],y[1])
		f.write(result)

def get_tweets():
        tags = ['cellular','network','mobile service','mobile phone','cell phone','cellphone','Cell network','cell service']
        reg_tags = [re.compile(x, re.IGNORECASE) for x in tags]
	title = "data/%cell_rumor.csv" % tag
	f = open(title, 'w')	#create new csv
        f.write('tweet id,tweet text')
	for y in reg_tags:
                raw_data = tweets.find({'text':y})
                for z in raw_data:
                        f.write('"%s",%d\n' % (z['user']['id'],z['text']))

if __name__ == "__main__":
    key_word_counter()
