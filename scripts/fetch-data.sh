#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Usage: fetch-data.sh [url file] [dest] [?ext]"
    exit
fi

EXT='.bz2'
if [ ! -z "$3" ]; then
    EXT="$3"
fi

DEST="$(dirname $2)/$(basename $2)"

while read LINE; do 
    wget -P $2 $LINE
    ARCHIVE="$DEST/$(basename $LINE)"
    lbzip2 -d "$ARCHIVE"
    NAME="$DEST/$(basename $LINE $EXT)"
    rename -f 'y/A-Z/a-z/'  $NAME
done < $1

