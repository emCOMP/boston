
import argparse
import sys
import os
import re
import codecs
import csv
from pymongo import MongoClient
import json

parser = argparse.ArgumentParser(description='whatevs')
parser.add_argument('host', help='host')
parser.add_argument('database', help='database name')
parser.add_argument('collection', help="collection name")
parser.add_argument('-l', '--limit', help="limit", type=int, default=0)
parser.add_argument('-o', '--output', help="outfile")
parser.add_argument('-f', '--filter', help="filter")

args = parser.parse_args()


client = MongoClient(args.host)
db = client[args.database]
col = None
if args.filter is not None:
	#print "filter: %s"%(args.filter)
	filter_dict = json.loads(args.filter, "latin-1")
	#print "filter dict: %s"%(json.dumps(filter_dict))
	col = db[args.collection].find(filter_dict, limit=args.limit)
else:
	col = db[args.collection].find(limit=args.limit)

print >> sys.stderr, col.count()
count = 0

if col.count() > 0:
	for row in col:
		creation = row["created_at"]
		code = row["codes"][0]["code"]
		#print >> sys.stderr, type(row["text"])
		text = row["text"]
		line = u"\"%s\",\"%s\",\"%s\"\n"%(creation, code, text)
		sys.stdout.write(line.encode('ascii','ignore'))
		count = count + 1
		if args.limit > 0 and count > args.limit:
			break

