#!/bin/bash
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?GROUP BY.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?HAVING.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM mulheresmil_1 WHERE/SELECT mulheresmil_1.number_of_records FROM mulheresmil_1 WHERE/g'

