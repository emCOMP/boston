#!/bin/bash

function run {
	echo $1...
	python tfidf2.py "data/$1_correction.csv" "data/$1_misinfo.csv" -t 100 > "out/$1_top_100_features.txt"
}

run jfk
run "girl running"
run proposal
run "cell phone"

