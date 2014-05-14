
import argparse
import sys
import os
import re
import codecs
import csv
from pymongo import MongoClient

parser = argparse.ArgumentParser(description='whatevs')
parser.add_argument('host', help='host')
parser.add_argument('database', help='database name')
parser.add_argument('collection', help="collection name")
parser.add_argument('-l', help="limit", type=int, default=-1)
args = parser.parse_args()


client = MongoClient(args.host)
db = client[args.database]
col = db[collection].find()

count = 0
for row in col:
	print "%s"%row.created_at
	count = count + 1
	if args.limit >= 0 and count > args.limit:
		break
