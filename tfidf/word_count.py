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
from time import gmtime
from datetime import timedelta, datetime
from nltk.tokenize.punkt import PunktWordTokenizer


parser = argparse.ArgumentParser(description='word count')
parser.add_argument('inputfiles', metavar='csv', nargs='+', help='input csv files')
parser.add_argument('-l', '--limit', help='limit number of lines', type=int, default=-1)
parser.add_argument('--max', help="top items", type=int, default=-1)
args = parser.parse_args()


re_strip_rt = re.compile(u'(RT|Via|MT|\(?from\s?\)?|) @\w+\:?\s', flags=re.IGNORECASE)
re_strip_mention = re.compile(u'(via\s)?@\w+', flags=re.IGNORECASE)
re_strip_cc = re.compile(u'-CC', flags=re.IGNORECASE)
re_strip_url = re.compile(u'(?i)\b((?:https?:?//|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))', flags=re.IGNORECASE)
re_strip_tco_url = re.compile(u'https?:?//[\w\-\.]*(/[\w\-\%]*)*( via)?', flags=re.IGNORECASE)
re_strip_punct = re.compile(u'[\.,;\:\-\!\[\]\"\?\(\)\|@\/]', flags=re.IGNORECASE)
re_strip_newline = re.compile(u'[\r\n\t]+')
re_strip_ending_http = re.compile(u'(\s)ht?t?t?p?:?/?/?([\w\-\.]*)?\s?$', flags=re.IGNORECASE)




last_time = datetime.now()
line_count = 0
last_line_count = 0


word_split_re = re.compile(u'\@?\w+|[\$?-]*[\d\.]+|[a-zA-Z0-9\-\']+|\p*', flags=re.IGNORECASE)
word_dict = {}

word_line_dict = {}

total_words = 0

print >> sys.stderr, "reading..."

for csvinput in args.inputfiles:
	print >> sys.stderr, "\t%s"%(csvinput)

	with open(csvinput, 'r') as infile:
		coder = codecs.iterencode(codecs.iterdecode(infile, "utf-8"), "utf-8")
		csvreader = csv.reader(coder, delimiter=',', quotechar='"')

		# skip header
		# next(csvreader)
		for row in csvreader:

			#row = [unicode(k,'utf-8', errors='ignore') for k in row]

			#print row[3]

			# strip out text
			text = row[2]

			# strip retweet
			text = re_strip_rt.sub(' ', text)
			text = re_strip_mention.sub(' ', text)
			text = re_strip_cc.sub(' ', text)
			text = re_strip_url.sub(' ', text)
			text = re_strip_tco_url.sub(' ', text)
			text = re_strip_ending_http.sub(' ', text)
			text = re_strip_punct.sub(' ', text)
			text = re_strip_newline.sub(' ', text)


			#words = text.lower().split()
			#words = word_split_re.split(text)
			words = word_split_re.findall(text)

			total_words += len(words)

			line_words = {}
			for word in words:
				word = word.lower()
				if len(word) == 0:
					continue

				if word not in word_dict:
					word_dict[word] = 1
				else:
					word_dict[word] += 1

				#if word not in line_words:
				#	line_words[word] = 1
				#	if word not in word_line_dict:
				#		word_line_dict[word] = 1
				#	else:
				#		word_line_dict[word] += 1


			# stop when necessary
			line_count += 1
			if args.limit > 0 and line_count >= args.limit:
				break

			# check time for update
			cur_time = datetime.now()
			delta = cur_time - last_time

			if delta.total_seconds() > 10:
				line_delta = line_count - last_line_count
				print >> sys.stderr, "%d  (%10.2f per sec)"%(line_count, (float(line_delta)/float(delta.total_seconds())))
				last_time = cur_time
				last_line_count = line_count


print >> sys.stderr, "sorting..."
sorted_dict = sorted(word_dict.iteritems(), key=operator.itemgetter(1), reverse=True)
if args.max >= 0:
	sorted_dict = sorted_dict[:args.max]
print >> sys.stderr, "dumping...\n\n"
print "%d, __total__"%total_words
for i in sorted_dict:
	print "%s,%s"%(i[1],i[0])
	#print "%s,%s,%s"%(i[1],i[0], word_line_dict[i[0]])
#print sorted_dict



