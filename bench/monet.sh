#!/bin/sh

if [ "$1" == "" ]; then
    exit 0
fi

for query in "${1%/}"/*.sql; do
    echo $query >> /dev/stdout;
    mclient -d imdb -t performance $query 2>>/dev/stdout 1>/dev/null
done
