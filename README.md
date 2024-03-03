# SkinnerDB

This repository contains a re-implementation of SkinnerDB, 
described in the SIGMOD 2019 paper <a href="https://dl.acm.org/citation.cfm?id=3275600">SkinnerDB: Regret-bounded query evaluation via reinforcement learning</a>. 

# Trying SkinnerDB on Google Colab

To get a first impression of SkinnerDB without installing it on your local machine, try the Google CoLab notebook [here](https://colab.research.google.com/drive/1P1Ekl-gdFwatIWSU5ZWajw82caM13te2?usp=sharing). The notebook downloads different SkinnerDB versions, benchmark data and workloads, executes SkinnerDB and analyzes the results. This notebook should **not be used** for benchmarking purposes as Google CoLab is not well suited to maximize SkinnerDB's performance (e.g., lack of native support for Java, few CPU cores making pre-processing slow etc.). 

# Running the Join Order Benchmark on EC2

The <a href="http://www.vldb.org/pvldb/vol9/p204-leis.pdf">join order benchmark</a> (JOB) is a popular benchmark for query optimizers. Follow these steps to benchmark SkinnerDB on JOB using an EC2 instance:

1. Create and connect to an EC2 instance. The following instructions have been tested on an EC2 t2.2xlarge instance running Ubuntu 22 with disk space enlarged to 32 GiB (compared to the default settings). After the login, stay in the home repository (/home/ubuntu).
2. Download this SkinnerDB repository and install Java:
```
git clone https://github.com/cornelldbgroup/skinnerdb
sudo apt-get update
sudo apt install openjdk-8-jre-headless
```
3. Install gdown and download a JOB database in the SkinnerDB format as .zip file from Google Drive:
```
sudo apt install python3-pip
sudo pip install gdown
gdown https://drive.google.com/uc?id=1UCXtiPvVlwzUCWxKM6ic-XqIryk4OTgE
```
4. Install unzip and use it to unzip the database file:
```
sudo apt install unzip
unzip imdbskinner.zip
```
5. Start SkinnerDB on the JOB database, using 16 GB of heap space and a specific garbage collector:
```
java -jar -Xmx16G -XX:+UseConcMarkSweepGC skinnerdb/jars/Skinner.jar skinnerimdb
```
6. After less than a minute, the SkinnerDB console should appear. Now, run a benchmark on the JOB queries using the following command:
```
bench skinnerdb/imdb/queries results.csv
```

After running the benchmark, benchmark results can be found in the specified output file. Benchmark results include per-query times for each of the processing phases (pre-processing, join phase, and post-processing) as well as many other statistics such as the number of tuples generated (column "Tuples") or the number of UCT tree nodes generated (column "NrUctNodes"). 

Running the benchmark should take around 104 seconds on the EC2 instance. Running it a second time (without leaving the SkinnerDB console) should only take around 83 seconds since SkinnerDB automatically caches indexes created on join columns. Optionally, you may index all columns in advance using the `index all` command in the SkinnerDB console.

# Quickstart

1. Create a new database using jars/CreateDB.jar or by executing tools/CreateDB.java. You need to specify two command line parameters: the database name and an (existing) directory in which the corresponding data is stored.

2. Start the Skinner console. The Skinner console can be accessed via jars/Skinner.jar or by executing console/SkinnerCmd.java. You need to specify the database directory as command line parameter (the same directory that was specified in the call to CreateDB.jar).

SkinnerDB is specialized for in-memory processing. The default settings for java JVM heap space etc. are in general insufficient. When starting the Skinner console, make sure to specify a sufficient amount of main memory using the -Xmx (and -Xms) parameters. Specifying more memory than necessary can improve performance further as it reduces the need for garbage collection. Also, to obtain more stable per-query performance, consider replacing the default garbage collector by specifying -XX:+UseConcMarkSweepGC as JVM parameter. Our recommendations are based on our experiences with our current test hardware and may not generalize to all platforms.

3. Create the database schema. SkinnerDB currently supports a limited number of SQL data types (text, int, and double). The example script located under imdb/skinner.schema.sql demonstrates how to create the schema of the join order benchmark. Note that you can execute commands in files via the 'exec <path>' command from the Skinner console. Run 'help' in the console to obtain a complete list of utility commands.

4. Load the data. SkinnerDB currently supports loading table data from CSV files. Run the command 'exec <table name> <separator> <path to .csv file> <representation of NULL values>' in the Skinner console to load data from the corresponding file into the specified table. The example script under 'imdb/skinner.load.sql' shows commands by which data for the join order benchmark can be loaded (assuming .csv files at the specified locations). The final command in that file refers to the next point.

5. (Optional) Compress string values after loading all data for all tables. Run the 'compress' command to create a dictionary that maps strings that appear in the database to integer code values. Processing integer values is significantly more efficient than processing strings. Compression may take a while as it iterates over the entire database. This pre-processing overhead may however pay off at run time.

6. Restart SkinnerDB (leave the console by entering 'quit' ).

7. (Optional) Create indices for the database columns. Run the 'index all' command in the Skinner console to create indices on all database columns. Again, this may take a while but can pay off at run time. Currently, we do not store indices on hard disk. This means that the 'index all' command (as opposed to the 'compress' command!) has to be re-run each time after starting the Skinner console.

8. Run analytical SQL queries. The current prototype only supports a very limited subset of SQL and not all features have been tested yet. The current support includes (without guarantees) select queries with inequality and equality predicates, LIKE expressions (as they appear in the join order benchmark, some special cases are currently not handled correctly), logical and arithmetic expressions, minimum and maximum aggregation, joins with predicates specified in the SQL WHERE clause, grouping, and sorting.

# Tuning

SkinnerDB includes various tuning parameters that can improve performance for specific benchmarks and data sets. Those parameters are hardcoded in the current version and can be found in the sub-folder src/config. Among the most important parameters are the `EXPLORATION_WEIGHT`, the `BUDGET_PER_EPISODE`, and the `FORGET` parameter (all in JoinConfig.java). Increasing the exploration weight makes the algorithm more "curious", thereby spending more effort in exploration as opposed to exploitation (here: using promising join orders). Increasing the budget per episode decreases learning overheads but may reduce the quality of join order decisions. If the forget parameter is enabled, the UCT tree is rebuilt regularly to increase exploration.

# Team

SkinnerDB is developed by the Cornell database group (https://research.cs.cornell.edu/).

# Citation

If you find this project inspiring or use our repository, please cite our journal paper.

```
@article{trummer2021skinnerdb,
  title={Skinnerdb: Regret-bounded query evaluation via reinforcement learning},
  author={Trummer, Immanuel and Wang, Junxiong and Wei, Ziyun and Maram, Deepak and Moseley, Samuel and Jo, Saehan and Antonakakis, Joseph and Rayabhari, Ankush},
  journal={ACM Transactions on Database Systems (TODS)},
  volume={46},
  number={3},
  pages={1--45},
  year={2021},
  publisher={ACM New York, NY}
}
```
