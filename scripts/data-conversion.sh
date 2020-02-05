#!/bin/bash

perl -pi -e 's/\|false/|0/g;' $1
perl -pi -e 's/\|true/|1/g;' $1
perl -pi -e 's/[^[:ascii:]]+//g' $1
