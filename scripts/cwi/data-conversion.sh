#!/bin/bash

CONTENT=`cat $1`

CONTENT=`echo "$CONTENT" | perl -pe 's/\|false/|0/g;'`
CONTENT=`echo "$CONTENT" | perl -pe 's/\|true/|1/g;'`
CONTENT=`echo "$CONTENT" | perl -pe 's/[^[:ascii:]]+//g'`

echo "$CONTENT"
