import argparse
import sys
import os
import re
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, TfidfTransformer
from sklearn.feature_selection import SelectKBest, chi2
import numpy
import codecs
import csv


parser = argparse.ArgumentParser(description='tfidf')
parser.add_argument('-i','--input', help='input csv file', required=True)
parser.add_argument('-l', '--limit', help='limit number of lines', type=int, default=-1)
parser.add_argument('-n', '--min', help='min threshold to show', type=int, default=1)
parser.add_argument('-x', '--max', help='max threshold to show', type=int, default=10000000)
parser.add_argument('-t', '--top', help='top x items', type=int, default=1000)
args = parser.parse_args()

line_count = 0

vectorizer = TfidfVectorizer(max_df=0.7, stop_words='english')
#vectorizer = CountVectorizer(max_df=0.5, stop_words='english')
line_regex = re.compile('^,\"\d*\",(.*)$')
word_split_re = re.compile(u'\s\p')
corpus = []

with codecs.open(args.input, "r", "utf-8") as infile:
	for line in infile:

		#print line

		# strip out text
		text = None
		r = line_regex.match(line)
		if r is not None:
			text = r.group(1)[1:-1]

		if text is not None and len(text) > 0:
			corpus.append(text)
			print text


		# stop when necessary
		line_count += 1
		if args.limit > 0 and line_count >= args.limit:
			break

	quit()

	print "transforming..."
	vec = vectorizer.fit_transform(corpus)

	freqs = [(word, vec.getcol(idx).sum()) for word, idx in vectorizer.vocabulary_.items()]


	print "sorting..."
	sorted_freqs = sorted(freqs, key=lambda x: -x[1])

	print "trimming to %d"%args.top
	trimmed = sorted_freqs[:args.top]

	for i,j in trimmed:
		if j > args.min and j < args.max:
			print "%s,%d"%(i,j)

