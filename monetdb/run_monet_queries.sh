#!/bin/sh
DBNAME=$1
USER=$2
DIR=$3

for query in $(find "$DIR" -name '*.sql' | sort)
do
        echo "=================MonetDB:$query======================="

mclient -d $DBNAME -t performance $query
        echo "\n\n"
done;