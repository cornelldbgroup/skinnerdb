#!/bin/bash
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?GROUP BY.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?HAVING.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM wins_1 WHERE/SELECT wins_1.number_of_records FROM wins_1 WHERE/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM wins_2 WHERE/SELECT wins_2.number_of_records FROM wins_2 WHERE/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM wins_3 WHERE/SELECT wins_3.number_of_records FROM wins_3 WHERE/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM wins_4 WHERE/SELECT wins_4.number_of_records FROM wins_4 WHERE/g'

