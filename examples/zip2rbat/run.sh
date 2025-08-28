#!/bin/sh

izname="./sample.d/sample.zip"

geninput(){
	echo generating input file...

	mkdir -p ./sample.d

	echo hw0 > ./sample.d/hw0.txt
	echo hw1 > ./sample.d/hw1.txt

	ls ./sample.d/hw?.txt |
		zip \
			-@ \
			-T \
			-v \
			-o \
			"${izname}"
}

test -f "${izname}" || geninput

export ZIP_FILENAME="${izname}"

./zip2rbat
