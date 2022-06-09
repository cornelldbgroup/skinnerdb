#!/bin/sh
# Timeout only works for MACOS
DBNAME=$1
DIR=$2

for query in $(find "$DIR" -name '*.sql' | sort)
do
        echo "=================MonetDB:$query======================="
        #mclient -u $USER -p monetdb -d $DBNAME -t performance $query
        gtimeout 100 mclient -d $DBNAME -t performance $query
        echo "\n\n"
done;
