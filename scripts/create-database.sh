#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: create-database.sh [database schema dir] [destination dir]"
    exit
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"

if [ ! -d "$1" ]; then
    echo "Error: $1 does not exist".
    exit
fi

if [ ! -d "$2" ]; then
    echo "Error: $2 does not exist".
    exit
fi

if test -f "$2/schema.sdb" ; then
    echo "Error: $2 already exists".
    exit
fi

if ! test -f "$DIR/build/CreateDb.jar" ; then
    echo "Error: DB Creation jar not found. Run mvn package -P CreateDb"
    exit
fi

SCHEMA="$(dirname $1)/$(basename $1)"
DEST="$(dirname $2)/$(basename $2)"

java -jar "$DIR/build/CreateDb.jar" "$(basename $SCHEMA)" $DEST
printf 'exec %s/skinner.schema.sql\nexec %s/skinner.load.sql\nquit' $SCHEMA $SCHEMA | $DIR/skinnerdb $DEST
