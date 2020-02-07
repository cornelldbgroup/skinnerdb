#!/bin/sh

CONTENT=`cat $1`

# Get rid of spaces/parens/slash/- in quotes
CONTENT=`echo "$CONTENT" | perl -pe 's:"[^"]*":($x=$&)=~s/\(|\)|-|\:|\///g;$x:ge'`
CONTENT=`echo "$CONTENT" | perl -pe 's:"[^"]*":($x=$&)=~s/ |\(|\)/_/g;$x:ge'`

# Get rid of non-ascii characters
CONTENT=`echo "$CONTENT" | perl -pe 's/[^[:ascii:]]+//g'`

# Convert to lower case
CONTENT=`echo "$CONTENT" | perl -pe 's/("[^"]*")/"\L\1"/g'`

# Get rid of quotes
CONTENT=`echo "$CONTENT" | perl -pe 's/"//g'`

# Type Change
CONTENT=`echo "$CONTENT" | perl -pe 's/BIGINT/LONG/g'`

echo "$CONTENT"

