import random

import numpy as np
art = "art"
col = "val"
table_num = 10
column_sizes = [1, 10, 100]
factor = 0.5

def create_db(art, col, num):
    dbnames = map(lambda x: art + str(x), range(num))
    db_txts = map(lambda name: "CREATE TABLE " + name + " (" + col + " integer NOT NULL);", dbnames)
    create_db_txt = "\n".join(db_txts)
    file = open('art/art.schema.sql', 'w+')
    file.write(create_db_txt)
    file.close()

def create_csv(art, num, size, factor):
    dbnames = map(lambda x: art + str(x), range(num))
    high = factor * 1000000
    for name in dbnames:
        file_name = name + ".csv"
        file = open('art/csv/' + str(size) + '/' + file_name, 'w+')
        for bid in range(size):
            for val in range(1000000):
                value = bid * 1000000 + val
                file.write("%d\n" % value)

        file.close()

def create_load(art, num, size):
    dbnames = list(map(lambda x: art + str(x), range(num)))
    load_texts = []
    for name in dbnames:
        file_name = "/Users/tracy/Documents/Research/skinnerdb/art/csv/" + str(size) + '/' + name + ".csv"
        load_texts.append("load " + name + " " + file_name + " NULL;")
    load_text = "\n".join(load_texts)
    file = open('art/art.load.' + str(size) + '.sql', 'w+')
    file.write(load_text)
    file.close()

def create_sql(art, col, num):
    dbnames = list(map(lambda x: art + str(x), range(num)))
    colnames = list(map(lambda x: col + str(x), range(num)))
    db_pairs = list(zip(dbnames, colnames))

    # select part
    select_txts = map(lambda pair: "MIN(" + pair[0] + "." + col + ") AS " + pair[1], db_pairs)
    select_txt = ", ".join(select_txts)

    # from part
    from_txts = map(lambda pair: pair[0] + " AS " + pair[0], db_pairs)
    from_txt = ", ".join(from_txts)

    # where part
    id_list = list(range(num))
    random.shuffle(id_list)
    where_txts = []
    while len(id_list) > 0:
        left = art + str(id_list.pop(0)) + "." + col
        right = art + str(id_list.pop(0)) + "." + col
        where_txts.append(left + " = " + right)
    where_txt = " AND ".join(where_txts)

    sql_text = "SELECT %s From %s WHERE %s;" % (select_txt, from_txt, where_txt)
    file = open('art/art.sql', 'w+')
    file.write(sql_text)
    file.close()

# print("Creating DB...")
# create_db(art, col, table_num)
print("Creating Data...")
for column_size in column_sizes:
    print("Size: " + str(column_size) + " MB")
    create_csv(art, table_num, column_size, factor)
# print("Creating Load...")
# create_load(art, table_num, 1)
# create_load(art, table_num, 10)
# create_load(art, table_num, 100)
# print("Creating SQL...")
# create_sql(art, col, table_num)


select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate


