#!/bin/bash
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?GROUP BY.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?HAVING.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM motos_1 WHERE/SELECT motos_1.number_of_records FROM motos_1 WHERE/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM motos_2 WHERE/SELECT motos_2.number_of_records FROM motos_2 WHERE/g'
