#!/bin/bash

rumours=("cell_phone" "girL_running" "jfk" "proposal" "seals_craft" "sunil")
codes=("correction" "misinfo" "hedge" "other_unclear_neutral" "question" "speculation")

function mkoutdir {
	# make dir if it doesn't exist
	if [[ ! -d "$1" ]] ;
		then mkdir "$1" ;
	fi
}

function run_wc {
	for code in ${codes[@]}; do
		echo ">> counting words:  $code"
		infile="data/$1_$code.csv"
		python word_count.py "$infile" > "$2/$code.csv"
	done
}

function run_tfidf {
	infiles=()
	for code in ${codes[@]}; do
		infiles+=("out/$1/${code}.csv")
	done

	outfile="out/$1/tfidf_${1}_all_codes.csv"

	python tfidf4.py "${infiles[@]}" --minthresh=0.0 >  "$outfile"

}


function run_wc_all {
	infiles=()
	for rumour in ${rumours[@]}; do
		infiles+=("data/${rumour}_$1.csv")
	done

	python word_count.py "${infiles[@]}" > "$2"
}

#function run_combined {
#	
#}



for rumour in ${rumours[@]}; do
	echo "$rumour ..."
	outdir="out/$rumour"

	mkoutdir "$outdir"
	run_wc "$rumour" "$outdir"
	run_tfidf "$rumour" "$outdir"
done


echo "Compiling all ..."
all_outdir="out/all"
mkoutdir "$all_outdir"
all_infiles=()
for code in ${codes[@]} ; do
	echo "${code}..."
	outfile="$all_outdir/$code.csv"
	run_wc_all "$code" "$outfile"
	all_infiles+=("$outfile")
done

echo "tfidfing all..."
echo "${all_infiles[@]}"
python tfidf4.py "${all_infiles[@]}" --minthresh=0.0 >  "$all_outdir/tfidf_all.csv"


