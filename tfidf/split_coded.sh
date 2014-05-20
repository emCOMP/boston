#!/bin/bash

function get_codes {
	fileprefix=`echo "$1" | sed "s/[\/[:space:][:blank:]]/_/g"`
	for code in "misinfo" "correction" "other/unclear/neutral" "question" "hedge" "speculation"
	do
		echo "> $code"
		codefile=`echo "$code" | sed "s/[\/[:space:][:blank:]]/_/g"`
		python mongo_get.py z new_boston tweets -f "{ \"codes.rumor\": \"$1\", \"codes.code\": \"${code}\"}" > "out/${fileprefix}_${codefile}.csv"
	done
}

function get_all {
	fileprefix=`echo "$1" | sed "s/[\/[:space:][:blank:]]/_/g"`
	python mongo_get.py z new_boston tweets -f "{ \"codes.rumor\": \"$1\"}" > "out/${fileprefix}_all.csv"
}


for i in "girl running" "cell phone" "jfk" "proposal" "seals/craft" "sunil"
do
	echo $i
	get_codes "$i"
	get_all "$i" 
done

