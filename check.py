import json
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
verify_tpch(20, "test")
# [7, 12, 11, 6, 9, 3, 13, 8, 10, 5, 4, 0, 1, 2]
# [83, 0, 0, 22544, 0, 0, 0, 0, 0, 93597, 267975, 523961, 840768, 730466]