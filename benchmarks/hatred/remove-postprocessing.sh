#!/bin/bash
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?GROUP BY.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?HAVING.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM hatred_1 WHERE/SELECT hatred_1.number_of_records FROM hatred_1 WHERE/g'

