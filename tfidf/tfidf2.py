import argparse
import sys
import os
import re
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, TfidfTransformer
from sklearn.feature_selection import SelectKBest, chi2
import numpy
import codecs
import csv
import numpy as np


line_count = 0
training_set = []
test_sets = None

def print_err(s):
	print >> sys.stderr, s


word_split_re = re.compile(u'\@?\w+|[\$?-]*[\d\.]+|[a-zA-Z0-9\-\']+|\p*', flags=re.IGNORECASE)

def tokenizer(s):
	words = [x for x in word_split_re.findall(s.lower()) if len(x) > 0]
	return words



parser = argparse.ArgumentParser(description='tfidf')
parser.add_argument('input1')
parser.add_argument('input2')
#parser.add_argument('-i','--input', help='input csv file', required=True)
parser.add_argument('-l', '--limit', help='limit number of lines', type=int, default=-1)
parser.add_argument('-n', '--min', help='min threshold to show', type=int, default=1)
parser.add_argument('-x', '--max', help='max threshold to show', type=int, default=10000000)
parser.add_argument('-t', '--top', help='top x items', type=int, default=1000)
args = parser.parse_args()



vectorizer = TfidfVectorizer(min_df=1, tokenizer=tokenizer, use_idf=True, stop_words=None, ngram_range=(1,3), analyzer=u'word')


def read_docs(filename, column):
	ret_arr = []
	#print_err("reading %s"%(filename))
	dupes = 0
	with open(filename, 'r') as infile:
		coder = codecs.iterencode(codecs.iterdecode(infile, "utf-8"), "utf-8")
		csvfile = csv.reader(coder, delimiter=',', quotechar='"')
		#next(csvfile)
		dup_checker = {}
		for row in csvfile:
			test = row[column].lower()
			if test not in dup_checker:
				dup_checker[test] = 1
				ret_arr.append(row[column])
			else:
				dupes += 1
	print_err("total dupes: %d"%dupes)

	return ret_arr


print_err("reading docs...")
doc1 = read_docs(args.input1, 2)
doc2 = read_docs(args.input2, 2)


training_set.extend(doc1)
training_set.extend(doc2)

labels = []
labels.extend([0]*len(doc1))
labels.extend([1]*len(doc2))



print_err("fit_transforming...")
train = vectorizer.fit_transform(training_set)

print_err("building large doc 1...")
doc1_lg = " ".join(doc1)
test_1 = vectorizer.transform(training_set)

print_err("extracting best features...")
ch2 = SelectKBest(chi2, k=args.top)
x_train = ch2.fit_transform(train, labels)
#x_test = ch2.transform(test_1)

feature_names = np.asarray(vectorizer.get_feature_names())

top_ranked_features = sorted(enumerate(ch2.scores_),key=lambda x:x[1], reverse=True)[:args.top]
top_ranked_features_indices = map(list,zip(*top_ranked_features))[0]
for feature_pvalue in zip(feature_names[top_ranked_features_indices],ch2.pvalues_[top_ranked_features_indices]):
        print "%s, %f"%(feature_pvalue[0], feature_pvalue[1])


