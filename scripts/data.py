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
            attr_list = list(map(lambda x: float(x[attr]), fileData[fname]))
            avg = sum(attr_list) / 1000 / len(attr_list) * 113
            value_list.append(str(avg))
        print("\t".join(value_list))
fileData = load_files("../data/cache")
agg_results(fileData, ["Millis", "PreMillis", "JoinMillis", "NrCacheMiss"])