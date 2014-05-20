#! /opt/local/bin/python
# -*- coding: utf-8 -*-
import argparse
import sys
import os
import re
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, TfidfTransformer
from sklearn.feature_selection import SelectKBest, chi2
import numpy
import codecs
import csv
import operator
import json
import math
from time import gmtime
from datetime import timedelta, datetime
from nltk.tokenize.punkt import PunktWordTokenizer


def getBaseFilename(file):
	return os.path.splitext(os.path.split(csvinput)[1])[0]


parser = argparse.ArgumentParser(description='word count')
parser.add_argument('inputfiles', metavar='csv', nargs='+', help='input csv files')
parser.add_argument('-l', '--limit', help='limit number of lines', type=int, default=-1)
parser.add_argument('--minthresh', help="minimum cutoff for all tfidf to meet to be included", type=float, default=0.01)
args = parser.parse_args()

last_time = datetime.now()
line_count = 0
last_line_count = 0



tweets = {}
word_dict = {}
documents = {}
total_counts = {}


# find base filenames
base_filenames = [getBaseFilename(csvinput) for csvinput in args.inputfiles]
inputs = dict( zip(args.inputfiles, base_filenames))


#
# calculate totals
#

print >> sys.stderr, "reading..."

for csvinput in args.inputfiles:

	doc = {}

	filename_base = inputs[csvinput]

	print >> sys.stderr, "\t%s"%(csvinput)

	with open(csvinput, 'r') as infile:
		coder = codecs.iterencode(codecs.iterdecode(infile, "utf-8"), "utf-8")
		csvreader = csv.reader(coder, delimiter=',', quotechar='"')

		total_sum = 0

		# skip totals
		total_words = int(next(csvreader)[0])
		#print >> sys.stderr, "\t\ttotal_words: %d"%total_words
		for row in csvreader:

			

			cnt = int(row[0])
			word = row[1]

			doc[word] = {}
			doc[word]['word'] = word
			doc[word]['freq'] =  cnt

			total_sum += cnt

			# increment the counts
			if word not in total_counts:
				total_counts[word] = {}
				total_counts[word]['word'] = word
				total_counts[word]['freq'] = cnt
				total_counts[word]['df'] = 1
				total_counts[word]['docs'] = {filename_base: cnt}
			else:
				total_counts[word]['freq'] += cnt
				total_counts[word]['df'] += 1
				total_counts[word]['docs'][filename_base] = cnt


			# check time for update
			cur_time = datetime.now()
			delta = cur_time - last_time

			if delta.total_seconds() > 10:
				line_delta = line_count - last_line_count
				print >> sys.stderr, "\t\t%d  (%10.2f per sec)"%(line_count, (float(line_delta)/float(delta.total_seconds())))
				last_time = cur_time
				last_line_count = line_count

		# add counts to our document collection
		documents[filename_base] = { 'words': doc, 'total_words': total_words, 'total_sum': total_sum }

#
# build new structure that's filtered
#

print >> sys.stderr, "calculating..."

filtered = {}
doc_cnt = float(len(documents))
line_count = 0
last_line_count = 0
for k,v in total_counts.iteritems():
	freq = int(v['freq'])
	if freq > 2:
		tmp = {}

		#print v['freq'], k, (freq > 2)

		docfreq = float(v['df'])
		idf = math.log( doc_cnt / (1+docfreq) )
		#print k, doc_cnt, docfreq, idf

		tmp['word'] = k
		tmp['freq'] = freq
		tmp['df'] = docfreq
		tmp['idf'] = idf

		include_row = False
		for f in base_filenames:
			total_doc_freq = documents[f]['total_sum']

			if f in v['docs']:
				total_doc_words = float(len(total_counts))

				# print total_doc_words
				# print v['docs']
				freq = float(v['docs'][f])
				#tf = freq / total_doc_words
				tf = freq / total_doc_freq
				tfidf = tf * idf
				tmp[f] = tfidf

				#print tfidf, type(tfidf), (tfidf > args.minthresh)
				#if tfidf > args.minthresh:
				include_row = True

			else:
				tmp[f] = 0

		if include_row:
			filtered[k] = tmp
			#print "included"

	line_count += 1

	# check time for update
	cur_time = datetime.now()
	delta = cur_time - last_time

	if delta.total_seconds() > 10:
		line_delta = line_count - last_line_count
		print >> sys.stderr, "\t\t%d  (%10.2f per sec)"%(line_count, (float(line_delta)/float(delta.total_seconds())))
		last_time = cur_time
		last_line_count = line_count



print >> sys.stderr, "dumping..."
cols = ['word', 'freq', 'df', 'idf']
cols.extend(base_filenames)

print ",".join(cols)
for k,v in filtered.iteritems():
	print ",".join( [str(v[col]) for col in cols] )



quit()

#
# calculate idf
#

print >> sys.stderr, "calculating idf..."

doc_cnt = float(len(documents))
for k,v in total_counts.iteritems():
	idf = math.log( doc_cnt / float(v['df']) )
	total_counts[k]['idf'] = idf


#for k,v in total_counts.iteritems():
#	print "%s %f %f"%(k,v['df'], v['idf'])
#	quit()

#
# calculate tf idf
#

print >> sys.stderr, "calculating tf-idf..."

for docname,doc in documents.iteritems():
	print >> sys.stderr, "\t%s"%(docname)

	total_doc_words = float(doc['total_words'])
	doc_dict = doc['words']
	for word,v in doc_dict.iteritems():
		freq = float(v['freq'])
		tf = freq / total_doc_words
		v['tf'] = tf
		v['term_freq'] = freq

		corp_count = total_counts[word]
		idf = corp_count['idf']
		df = corp_count['df']
		corp_freq = corp_count['freq']

		v['idf'] = idf
		v['tfidf'] = tf * idf
		v['df'] = df
		v['corp_freq'] = float(corp_freq)
			
		# check time for update
		cur_time = datetime.now()
		delta = cur_time - last_time

		if delta.total_seconds() > 10:
			line_delta = line_count - last_line_count
			print >> sys.stderr, "\t\t%d  (%10.2f per sec)"%(line_count, (float(line_delta)/float(delta.total_seconds())))
			last_time = cur_time
			last_line_count = line_count


print >> sys.stderr, "sorting..."
for docname,doc in documents.iteritems():
	print >> sys.stderr, "\t%s"%(docname)

	doc_dict = doc['words']
	sorted_dict = sorted(doc_dict.values(), key=operator.itemgetter('tfidf'), reverse=True)

	if args.max >= 0:
		sorted_dict = sorted_dict[:args.max]
	print >> sys.stderr, "dumping...\n\n"
	print "filename, word, corp_freq, df, idf, term_freq, tf, tfidf"
	for i in sorted_dict:
		print "%s,%s,%f,%f,%f,%f,%f,%f"%(docname, i['word'], i['corp_freq'], i['df'], i['idf'], i['term_freq'], i['tf'], i['tfidf'])
	#print sorted_dict



