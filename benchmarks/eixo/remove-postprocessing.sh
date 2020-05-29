#!/bin/bash
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?GROUP BY.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/[ ]?HAVING.*;/;/g'
find . -name "*.sql" | xargs perl -pi -e 's/SELECT.*FROM eixo_1 WHERE/SELECT eixo_1.number_of_records FROM eixo_1 WHERE/g'

