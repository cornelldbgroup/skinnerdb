#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: data-conversion.sh [input file]"
    exit
fi

perl -pi -e 's/\|false/|0/g;' $1
perl -pi -e 's/\|true/|1/g;' $1
perl -pi -e 's/[^[:ascii:]]+//g' $1
