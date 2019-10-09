import csv
import os
def load_csv(fname):
    with file(fname) as f:
        dialect = csv.Sniffer().sniff(f.read(2024))
        f.seek(0)
        reader = csv.reader(f, dialect)
        header = reader.next()
        data = [dict(zip(header, l)) for l in reader]
    return data

def analyze_log(fname):
    max_time = 0
    time_list = []
    with open(fname) as f:
        for line in f.readlines():
            if line.startswith("Episode Time"):
                line = line.rstrip().split(": ")[1]
                actual_time = int(line)
                max_time = max(max_time, int(line))
                if actual_time > 0:
                    time_list.append(actual_time)
    print(max_time)
    print(sum(time_list))
    print(time_list)


def load_files(dir):
    rd = {}
    for file in os.listdir(dir):
        if file.endswith(".txt"):
            result = load_csv(os.path.join(dir, file))
            rd[file] = result
    return rd

def agg_results(fileData, attrs):
    print("Sys\t" + "\t".join(attrs))
    for fname in fileData:
        name = fname[:-4]
        value_list = [name]
        for attr in attrs:
            if attr in fileData[fname][0]:
                attr_list = list(map(lambda x: float(x[attr]), fileData[fname]))
                avg = sum(attr_list) / 1000 / len(attr_list) * 113
                value_list.append(str(avg))
            else:
                value_list.append("NA")
        print("\t".join(value_list))
fileData = load_files("../data/parallelization")
agg_results(fileData, ["Millis", "PreMillis", "ExeMillis", "PostMillis", "FilterMillis", "IndexMillis"])
# analyze_log("../data/logs/1.txt")