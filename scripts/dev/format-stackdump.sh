#!/bin/bash

function format() {
  sed \
    -e "s#$GOPATH#\$GOPATH#g" \
    -e 's#$GOPATH#\n$GOPATH#g' \
    -e 's#<body>#\n<body>\n#' \
    -e 's#</body>#\n</body>#' \
    -e 's# /usr/lib/go/src#\n/usr/lib/go/src#g'
}



if [ -n "$1" ]; then
	if [ "$1" == "-h" -o "$1" == "--help" ]; then
		echo "$0: reads an unformatted stackdump, and formats it"
		echo "replaces your GOPATH with the string \#GOPATH and introduces newlines where appropriate"
		echo "usage:"
		echo "$0 --help, -h     this message"
		echo "$0 <filename>     reads input from file"
		echo "$0                reads input from stdin"
		exit 0
	elif [ -r "$1" ]; then
		format < "$1"
		exit 0
	else
		echo "unrecognized input flag: $1 (not a file)"
		exit 2
	fi
fi

format
