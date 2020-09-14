import json
import re
def analyze_file(filename):
    bingo_result = set()
    with open(filename) as f:
        for line in f:
            if line.startswith("INFO:Bingo: "):
                result_str = line.split(": ")[1].rstrip().split("]")[0] + "]"
                result_list = json.loads(result_str)
                bingo_result.add(result_str)
        return bingo_result

def getList(line):
    numbers = line.split("[")[1].split("]")[0]
    return '[' + numbers + ']'

# extract result table from the file in Skinner format
def extract_result(filename):
    query_results = {}
    with open(filename) as f:
        query_name = ""
        result_list = []
        query_start = False
        dash_ctr = 0
        for line in f:
            if line.rstrip().endswith(".sql"):
                query_name = line.split(".")[0]
                result_list = []
                dash_ctr = 0
                query_start = True
            if query_start and line.startswith("---"):
                dash_ctr += 1
                if dash_ctr == 3:
                    if len(result_list) > 0 and len(result_list[0]) > 0 and result_list[0][0] != "[null]":
                        query_results[query_name] = result_list
                continue
            if dash_ctr == 2:
                line_list = line.rstrip().split("\t")
                result_list.append(line_list)


    return query_results

# extract result table from the file in MonetDB format
def extract_monet_result(filename):
    query_results = {}
    with open(filename) as f:
        query_name = ""
        result_list = []
        query_start = False
        dash_ctr = 0
        for line in f:
            if line.rstrip().startswith("================="):
                query_name = line.rstrip().split(".sql")[0].split("/")[-1]
                result_list = []
                dash_ctr = 0
                query_start = True
            if query_start and line.startswith("+="):
                dash_ctr += 1
                continue
            if dash_ctr == 1:
                if line.startswith("+-"):
                    dash_ctr +=1
                    if len(result_list) > 0 and (result_list[0][0] != "null"):
                        query_results[query_name] = result_list
                    continue
                line_list = list(line.rstrip().split("|"))
                line_list = list(filter(lambda s: s != '', list(map(lambda x: x.strip(), line_list))))
                result_list.append(line_list)


    return query_results

def findRoundCtr(filename, example):
    round_list = []
    with open(filename) as f:
        roundCtr = 0
        lines = f.readlines()
        x = 0
        while x < len(lines):
            line = lines[x]
            if line.startswith("Round:"):
                roundCtr += 1
            if line.startswith("Join:"):
                joinOrder = json.loads(getList(line))
                start = json.loads(getList(lines[x+1]))
                j = x + 2
                while not lines[j].startswith("End"):
                    j += 1
                end = json.loads(getList(lines[j]))
                found = True
                for table in joinOrder:
                    tupleIndex = example[table]
                    if tupleIndex > start[table] and tupleIndex < end[table]:
                        break
                    elif tupleIndex == start[table] or tupleIndex == end[table]:
                        continue
                    else:
                        found = False
                        break
                x = j
                if found:
                    return roundCtr
                continue
            x += 1
    return -1

def verify(nrThreads, sql):
    file_num = nrThreads
    test = set()
    sqlName = sql
    # baseline = analyze_file("/Users/tracy/Documents/vocalization/mcts_db_tests/logs/verbose/seq/" + sqlName + ".log")
    baseline = analyze_file("/Users/tracy/Documents/vocalization/mcts_db_tests/logs/verbose/seq/26c.log")
    for x in range(0, file_num, 1):
        file_result = analyze_file("../data/logs/verbose/root/" + sqlName + "/" + str(x) + ".txt")
        for result in file_result:
            test.add(result)
    print(str(len(test)) + " " + str(len(baseline)))
    lack = baseline.difference(test)
    if len(lack) == 0:
        print("[Pass]")
    else:
        print(lack)
        exp = json.loads(list(lack)[0])

        for x in range(0, file_num, 1):
            roundCtr = findRoundCtr("../data/logs/verbose/root/" + sqlName + "/" + str(x) + ".txt", exp)
            print("Thread " + str(x) + ": " + str(roundCtr))

def verify_tpch(nrThreads, sql):
    file_num = nrThreads
    test = set()
    sqlName = sql
    baseline = analyze_file("/Users/tracy/Documents/Research/skinnertpch/skinnerdb/logs/verbose/lockFree/test/0.txt")
    # for x in range(0, file_num, 1):
    #     file_result = analyze_file("/Users/tracy/Documents/Research/logs/logs/test/" + str(x) + ".txt")
    #     for result in file_result:
    #         test.add(result)
    file_result = analyze_file("/Users/tracy/Documents/Research/skinnerdb/log.txt")
    for result in file_result:
        test.add(result)
    print("baseline: " + str(len(baseline)) + "; new: " + str(len(test)))
    lack = baseline.difference(test)
    if len(lack) == 0:
        print("[Pass]")
    else:
        print(lack)
        exp = json.loads(list(lack)[0])

        for x in range(0, file_num, 1):
            roundCtr = findRoundCtr("/Users/tracy/Documents/Research/logs/logs/test/" + str(x) + ".txt", exp)
            print("Thread " + str(x) + ": " + str(roundCtr))
# verify(20, "test")
# verify_tpch(20, "test")
# [7, 12, 11, 6, 9, 3, 13, 8, 10, 5, 4, 0, 1, 2]
# [83, 0, 0, 22544, 0, 0, 0, 0, 0, 93597, 267975, 523961, 840768, 730466]


# verify the final query results
# @param base_file  monetdb result file
# @param skinner_file  skinnerdb result file
def verify_final_result(base_file, skinner_file):
    SIMPLE_FLOAT_REGEXP = re.compile(r'^[-+]?[0-9]+\.?[0-9]+([eE][-+]?[0-9]+)?$')
    # skinner = extract_result("./skinnerResults.txt")
    # base = extract_monet_result("./monetResults.txt")
    skinner = extract_result(skinner_file)
    base = extract_monet_result(base_file)

    for query_name in base:
        print("Verifying query " + query_name + "...")
        skinner_result_list = skinner[query_name]
        base_result_list = base[query_name]
        for x in range(len(base_result_list)):
            base_line = base_result_list[x]
            if x >= len(skinner_result_list):
                print("Fatal error: " + "|".join(base_line) + " not exists at Line " + str(x))
                return
            skinner_line = skinner_result_list[x]

            for index in range(len(base_line)):
                base_element = base_line[index]
                skinner_element = skinner_line[index]
                if SIMPLE_FLOAT_REGEXP.match(base_element):
                    base_element = "%.2f" % round(float(base_element), 5)
                    if not SIMPLE_FLOAT_REGEXP.match(skinner_element):
                        print(
                            "Fatal error: " + "|".join(skinner_line) + " -> " + "|".join(base_line) + " at Line " + str(
                                x))
                        # return
                    skinner_element = "%.2f" % round(float(skinner_element), 5)
                    if base_element != skinner_element:
                        print(
                            "Fatal error: " + "|".join(skinner_line) + " -> " + "|".join(base_line) + " at Line " + str(
                                x))
                        # return
                else:
                    if base_element.strip() != skinner_element.strip():
                        print("Fatal error: " + "|".join(skinner_line) + " -> " + "|".join(base_line) + " at Line " + str(x))
                        # return

        print("[Pass]")

verify_final_result("./tpchx_monet_results.txt", "./tpchx_skinner_results.txt")
