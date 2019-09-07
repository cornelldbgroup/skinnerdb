import pymonetdb
import csv
prefix = "/Users/tracy/Documents/Research/skinnerdb/imdb/csv/"

def db_schema():
    schema = {}
    with open("/Users/tracy/Documents/Research/skinnerdb/imdb/skinner.schema.sql") as database:
        table_name = ""
        for line in database:
            if line.startswith("CREATE TABLE"):
                table_name = line.split(" ")[2]
                schema[table_name] = []
            elif line.startswith("    "):
                type = line.lstrip().split(" ")[1]
                schema[table_name].append(type == "integer")

    return schema

def import_csv(file_name):
    schema = db_schema()
    connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="imdb")
    cursor = connection.cursor()
    with open(prefix + file_name + ".csv", encoding="utf8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        sql = "COPY INTO %s FROM STDIN;\n" % (file_name)
        data = []
        for row in csv_reader:
            table_schema = schema[file_name]
            for x in range(len(table_schema)):
                if not table_schema[x]:
                    row[x] = '"' + row[x] + '"'
            subdata = ",".join(row)
            data.append(subdata)
        data_sql = "\n".join(data)
        sql = sql + data_sql + ";"
        cursor.execute(sql)


import_csv("kind_type")
