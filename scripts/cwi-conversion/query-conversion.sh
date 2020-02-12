#!/bin/sh

if [ "$#" -ne 1 ]; then
    echo "Usage: data-conversion.sh [input file]"
    exit
fi

CONTENT=`cat $1`

# Get rid of spaces/parens/slash/- in quotes
CONTENT=`echo "$CONTENT" | perl -pe 's:"[^"]*":($x=$&)=~s/\(|\)|-|\:|\///g;$x:ge'`
CONTENT=`echo "$CONTENT" | perl -pe 's:"[^"]*":($x=$&)=~s/ |\(|\)/_/g;$x:ge'`

# Get rid of non-ascii characters
CONTENT=`echo "$CONTENT" | perl -pe 's/[^[:ascii:]]+//g'`

# Convert to lower case
CONTENT=`echo "$CONTENT" | perl -pe 's/("[^"]*")/"\L\1"/g'`

# Get rid of quotes/question mark
CONTENT=`echo "$CONTENT" | perl -pe 's/"//g'`
CONTENT=`echo "$CONTENT" | perl -pe 's/\?//g'`

# Type Change
CONTENT=`echo "$CONTENT" | perl -pe 's/BIGINT/LONG/g'`
CONTENT=`echo "$CONTENT" | perl -pe 's/smallint|boolean/int/g'`
CONTENT=`echo "$CONTENT" | perl -pe 's/decimal\(.*\)/double/g'`

echo "$CONTENT"

