import math
import random
import re
import os
import json


def count_equals(query):
    p = re.compile("\S+\.\S+\s*=\s*'")
    count = 0
    for m in p.finditer(query):
        count += 1
    return count


def reformulate_query_in(query, flags):
    p = re.compile("\S+\.\S+\s*=\s*'")
    predicates = []
    columns = []
    values = []
    for m in p.finditer(query):
        pos = m.end()
        while True:
            if query[pos] == "'":
                predicate = query[m.start():pos+1]
                column = predicate.split("=")[0].strip()
                value = predicate.split("=")[1].strip()
                print(value)
                columns.append(column)
                values.append(value)
                predicates.append(predicate)
                break
            pos += 1
    for x in range(len(predicates)):
        if flags[x]:
            new_predicate = columns[x] + " in (" + values[x] + ")"
            query = query.replace(predicates[x], new_predicate)
    return query

def reformulate_query_like(query, flags):
    p = re.compile("\S+\.\S+\s*=\s*'")
    predicates = []
    columns = []
    values = []
    for m in p.finditer(query):
        pos = m.end()
        while True:
            if query[pos] == "'":
                predicate = query[m.start():pos+1]
                column = predicate.split("=")[0].strip()
                value = predicate.split("=")[1].strip()
                print(value)
                columns.append(column)
                values.append(value)
                predicates.append(predicate)
                break
            pos += 1
    for x in range(len(predicates)):
        if flags[x]:
            new_predicate = columns[x] + " like '" + values[x][1:-2] + "%'"
            replaced_predicate = new_predicate + " AND " + predicates[x]
            query = query.replace(predicates[x], replaced_predicate)
    return query


def reformulate_query_duplicate(query):
    p = re.compile("\S+\.\S+\s*=\s*'")
    predicates = []
    columns = []
    values = []
    max_dus = 10

    for m in p.finditer(query):
        pos = m.end()
        while True:
            if query[pos] == "'":
                predicate = query[m.start():pos+1]
                column = predicate.split("=")[0].strip()
                value = predicate.split("=")[1].strip()
                print(value)
                columns.append(column)
                values.append(value)
                predicates.append(predicate)
                break
            pos += 1
    for x in range(len(predicates)):
        nr_dus = random.randint(1, max_dus)
        duplicated_predicates = [predicates[x]] * nr_dus
        new_predicate = " AND ".join(duplicated_predicates)
        replaced_predicate = new_predicate + " AND " + predicates[x]
        query = query.replace(predicates[x], replaced_predicate)
    return query


def read_queries(query_path):
    with open(query_path) as f:
        lines = f.readlines()
        return "\n".join(lines)


def reformulate(probability):
    prefix = "/Users/tracy/Documents/Research/skinnerdb/imdb/imdb_monet_queries/"
    output_prefix = f"/Users/tracy/Documents/Research/skinnerdb/imdb/queries_du_{probability}/"
    if not os.path.exists(output_prefix):
        os.makedirs(output_prefix)
    counts = {}
    all_counts = 0
    for file in os.listdir(prefix):
        if file.endswith(".sql"):
            query_path = os.path.join(prefix, file)
            print(query_path)
            query = read_queries(query_path)
            query_count = count_equals(query)
            counts[file] = query_count
            all_counts += query_count

    change_counts = int(round(all_counts * probability))
    count_list = []
    for x in range(all_counts):
        element = 1 if x < change_counts else 0
        count_list.append(element)
    random.shuffle(count_list)

    start = 0
    for file in os.listdir(prefix):
        if file.endswith(".sql"):
            query_path = os.path.join(prefix, file)
            print(file)
            query = read_queries(query_path)
            query_count = counts[file]
            changed_query = reformulate_query_duplicate(query)
            with open(f"{output_prefix}{file}", 'w') as f:
                f.write(changed_query)
            print(changed_query)
            start += query_count


def query_graph(query):
    tables = [list(map(lambda x: x.strip(), exp.split("AS"))) for exp in re.findall(r"\w+ AS \w+", query)]
    alias_set = [table[1] for table in tables]
    # Find BETWEEN left AND right
    betweens = set(re.findall(r"BETWEEN \d+ AND \d+", query) + re.findall(r"BETWEEN '\w+' AND '\w+'", query))
    for between in betweens:
        replacement = between.replace("BETWEEN", "between").replace("AND", "and")
        query = query.replace(between, replacement)
    predicates = list(map(lambda x: x.strip(), query.split("WHERE")[1].replace(";", "").split(" AND ")))
    combine = False
    nr_left = 0
    nr_right = 0
    combine_list = []
    added_predicates = []
    removed_predicates = []
    for predicate in predicates:
        nr_left += predicate.count('(')
        nr_right += predicate.count(')')
        if nr_left != nr_right:
            combine = True
            combine_list.append(predicate)
            removed_predicates.append(predicate)
        if combine and nr_left == nr_right:
            combine = False
            combine_list.append(predicate)
            removed_predicates.append(predicate)
            combine_predicates = " AND ".join(combine_list)
            added_predicates.append(combine_predicates)
            combine_list = []
            nr_left = 0
            nr_right = 0

    for predicate in removed_predicates:
        predicates.remove(predicate)

    for predicate in added_predicates:
        predicates.append(predicate)

    predicates = set(predicates)
    selects = query.split("WHERE")[0].strip()
    unary_predicates_alias = {alias: [] for alias in alias_set}
    join_predicates_alias = {alias: [] for alias in alias_set}
    join_columns = {alias: set() for alias in alias_set}
    join_predicates = set(filter(lambda exp: re.match(r"\w+\.\w+\s*=\s*\w+\.\w+", exp) is not None, predicates))
    unary_predicates = predicates.difference(join_predicates)

    # Add unary predicates to a map
    for unary_predicate in unary_predicates:
        unary_table = re.findall(r"\w+\.\w+", unary_predicate)[0]
        unary_alias = unary_table.split(".")[0].strip()
        unary_predicates_alias[unary_alias].append(unary_predicate)

    # Join graph
    for join_predicate in join_predicates:
        tables = join_predicate.split("=")
        left = tables[0].strip().split(".")[0]
        right = tables[1].strip().split(".")[0]
        left_col = tables[0].strip().split(".")[1]
        right_col = tables[1].strip().split(".")[1]
        join_predicates_alias[left].append(right)
        join_predicates_alias[right].append(left)
        join_columns[left].add(left_col)
        join_columns[right].add(right_col)
    return unary_predicates_alias, join_predicates_alias, join_columns, selects, list(join_predicates)


def retrieve_cards(path):
    cards_map = {}
    with open(path) as f:
        for line in f.readlines():
            line = line.strip()
            if line.endswith(".sql"):
                query_name = line
                new_query = True
            elif line.startswith("Finish Parallel Join!"):
                new_query = False
            elif line.startswith("Alias:") and not new_query:
                alias = line.split(": ")[1][1:-1]
                alias_list = alias.split(",")
                alias = [x.strip() for x in alias_list]
            elif line.startswith("Cards:") and not new_query:
                cards = line.split(": ")[1][1:-1]
                card_list = cards.split(",")
                cards = [int(x.strip()) for x in card_list]
                zip_iterator = zip(alias, cards)
                card_dict = dict(zip_iterator)
                cards_map[query_name] = card_dict
    return cards_map


def load_queries(path):
    queries = {}
    for file in os.listdir(path):
        if file.endswith(".sql"):
            query_path = os.path.join(path, file)
            query = read_queries(query_path)
            queries[file] = query
    return queries


def generate_duplicate_queries(query, cards, query_name):
    duplicates = [query]
    alias_list = list(query_graph(query)[0].keys())
    query_orders = []
    if cards is not None:
        hard_order = sorted(alias_list.copy(), key=lambda x: cards[x], reverse=True)
        easy_order = sorted(alias_list.copy(), key=lambda x: cards[x])
        query_orders += [hard_order, easy_order]
    for x in range(9 - len(query_orders)):
        alias_copy = alias_list.copy()
        random.shuffle(alias_copy)
        query_orders.append(alias_copy)
    nr_joined = len(alias_list)
    nr_predicates = 0
    nr_duplicates = 5
    total_predicates = 20

    for query_order in query_orders:
        unary_predicates, join_predicates, join_columns, selects, joins = query_graph(query)
        unarys = []
        # for table_ctr in range(nr_joined):
        #     table_alias = query_order[nr_joined - 1 - table_ctr]
        #     table_unary_predicates = unary_predicates[table_alias]
        #     nr_table_predicates = len(table_unary_predicates)
        #     while nr_table_predicates <= nr_predicates:
        #         if len(table_unary_predicates) == 0:
        #             join_column = next(iter(join_columns[table_alias]))
        #             table_unary_predicates.append(f"{table_alias}.{join_column} >= 0")
        #         else:
        #             table_unary_predicates.append(table_unary_predicates[0])
        #         nr_table_predicates = len(table_unary_predicates)
        #     unarys += table_unary_predicates
        #     nr_predicates = nr_table_predicates
        for table_alias in query_order:
            table_unary_predicates = unary_predicates[table_alias]
            if nr_predicates < total_predicates:
                for d in range(nr_duplicates):
                    if len(table_unary_predicates) == 0:
                        join_column = next(iter(join_columns[table_alias]))
                        table_unary_predicates.append(f"{table_alias}.{join_column} >= 0")
                    else:
                        table_unary_predicates.append(table_unary_predicates[0])
                nr_predicates += nr_duplicates
            unarys += table_unary_predicates

        du_unarys = " AND ".join(unarys)
        du_joins = " AND ".join(joins)
        duplicate_query = f"{selects} WHERE {du_unarys} AND {du_joins};"
        duplicates.append(duplicate_query)
    return duplicates


def shuffle_duplicates(dataset):
    output_dirs = []
    for x in range(10):
        order = x + 1
        output_prefix = f"/Users/tracy/Documents/Research/skinnerdb/{dataset}/queries_shuffle_{str(order)}/"
        if not os.path.exists(output_prefix):
            os.makedirs(output_prefix)
        output_dirs.append(output_prefix)

    card_path = "/Users/tracy/Documents/Research/skinnerdb/logs/test/cards.log"
    cards_queries = retrieve_cards(card_path)

    query_path = f"/Users/tracy/Documents/Research/skinnerdb/{dataset}/{dataset}_monet_queries/"
    queries = load_queries(query_path)
    query_keys = sorted(queries.keys())
    for query_key in query_keys:
        print(f"Constructing query {query_key}...")
        cards = cards_queries[query_key] if query_key in cards_queries else None
        duplicates = generate_duplicate_queries(queries[query_key], cards, query_key)
        for idx, duplicate in enumerate(duplicates):
            with open(f"{output_dirs[idx]}{query_key}", 'w') as f:
                f.write(duplicate)



# reformulate(1)
# reformulate(2)
# reformulate(3)
# reformulate(4)
# reformulate(5)
# reformulate(6)
# reformulate(7)
# reformulate(8)
# reformulate(9)
# reformulate(0)
# query = "SELECT MIN(cn.name) AS producing_company, MIN(lt.link) AS link_type, MIN(t.title) AS complete_western_sequel " \
#             "FROM complete_cast AS cc, comp_cast_type AS cct1, comp_cast_type AS cct2, company_name AS cn, " \
#             "company_type AS ct, keyword AS k, link_type AS lt, movie_companies AS mc, movie_info AS mi, movie_keyword AS mk, " \
#             "movie_link AS ml, title AS t WHERE cct1.kind  = 'cast' AND cct2.kind  like 'complete%' " \
#             "AND cn.country_code <>'[pl]' AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%') AND ct.kind ='production companies' " \
#             "AND k.keyword ='sequel' AND lt.link LIKE '%follow%' AND mc.note IS NULL " \
#             "AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark', 'Swedish', 'Denish', 'Norwegian', 'German', 'English') " \
#             "AND t.production_year BETWEEN 1950 AND 2010 AND t.production_year BETWEEN 1950 AND 2010 " \
#             "AND t.production_year BETWEEN 1950 AND 2010 AND t.production_year BETWEEN 1950 AND 2010 " \
#             "AND mi.movie_id >= 0 AND mi.movie_id >= 0 AND mi.movie_id >= 0 ANd mi.movie_id >= 0 " \
#             "AND lt.id = ml.link_type_id AND ml.movie_id = t.id AND t.id = mk.movie_id AND mk.keyword_id = k.id " \
#             "AND t.id = mc.movie_id AND mc.company_type_id = ct.id AND mc.company_id = cn.id AND mi.movie_id = t.id " \
#             "AND t.id = cc.movie_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id AND ml.movie_id = mk.movie_id " \
#             "AND ml.movie_id = mc.movie_id AND mk.movie_id = mc.movie_id AND ml.movie_id = mi.movie_id AND mk.movie_id = mi.movie_id " \
#             "AND mc.movie_id = mi.movie_id " \
#             "AND ml.movie_id = cc.movie_id AND mk.movie_id = cc.movie_id AND mc.movie_id = cc.movie_id AND mi.movie_id = cc.movie_id;"
# query_graph(query)
shuffle_duplicates("imdb")