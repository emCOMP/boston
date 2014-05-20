#! /opt/local/bin/python
# -*- coding: utf-8 -*-

import argparse
import sys
import os
import re
import numpy
import codecs
import csv
import operator
import json
import math
from time import gmtime
from datetime import timedelta, datetime


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
re_strip_punct = re.compile(u'[\.,;\:\-\!\[\]\"\'\?\(\)\|@\/]', flags=re.IGNORECASE)
re_strip_newline = re.compile(u'[\r\n\t]+')
re_strip_ending_http = re.compile(u'(\s)ht?t?t?p?:?/?/?([\w\-\.]*)?\s?$', flags=re.IGNORECASE)

word_split_re = re.compile(u'\@?\w+|[\$?-]*[\d\.]+|[a-zA-Z0-9\-\'\/]+|[!\?\.\,\/\@\:\-\_&\$\s]*', flags=re.IGNORECASE)



for csvinput in args.inputfiles:

	with open(csvinput, 'r') as infile:
		coder = codecs.iterencode(codecs.iterdecode(infile, "utf-8"), "utf-8")
		csvreader = csv.reader(coder, delimiter=',', quotechar='"')

		# skip header
		next(csvreader)
		for row in csvreader:

			text = row[2]


			# strip retweet
			text = re_strip_rt.sub('', text)
			text = re_strip_mention.sub('', text)
			text = re_strip_cc.sub('', text)
			text = re_strip_url.sub('', text)
			text = re_strip_tco_url.sub('', text)
			text = re_strip_ending_http.sub('', text)
			text = re_strip_punct.sub('', text)
			text = re_strip_newline.sub(' ', text)


			print text





