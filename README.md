# SkinnerDB

This repository contains a very early version of a (slightly refined) re-implementation of SkinnerDB,
described in the paper <a href="https://dl.acm.org/citation.cfm?id=3275600">SkinnerDB: Regret-bounded query evaluation via reinforcement learning</a> at SIGMOD 2019 (see video recording of SIGMOD talk <a href="https://www.youtube.com/watch?v=QRYVnKaZ9fw">here</a>).

This source code is currently under development and **NOT CONSIDERED STABLE**.

We expect to release the first stable version in the next months.

# Running the Join Order (IMDB) Benchmark

The <a href="http://www.vldb.org/pvldb/vol9/p204-leis.pdf">join order benchmark</a> is a popular benchmark for query optimizers. Execute the following steps to run SkinnerDB on that benchmark:

<ol>
<li>Download the IMDB database in the SkinnerDB format <a href="https://drive.google.com/file/d/1UCXtiPvVlwzUCWxKM6ic-XqIryk4OTgE/view?usp=sharing">here</a>. Decompress the linked .zip file.</li>
<li>Download this GitHub repository (the master branch!) which already contains the join order benchmark queries in the sub-folder imdb/queries.</li>
<li>Run `mvn package` to compile the code.</li>
<li>Start SkinnerDB using the executable .jar file in the target sub-folder. For Linux, use the following command in the jars directory (while replacing /path/to/skinner/data by the path to the decompressed IMDB database):
<p>
<code>
java -jar -Xmx16G -XX:+UseConcMarkSweepGC Skinner.jar /path/to/skinner/data
</code>
</p>
The settings for garbage collector (<code>-XX:+UseConcMarkSweepGC</code>) and heap space (<code>-Xmx16G</code>) work best for our benchmarking platform but may need to be revised for different machines.
</li>
<li>Optionally, create indexes on all columns using the <code>index all</code>
command in the SkinnerDB console. Note that you need to re-create indexes after each startup as the current version does not store indexes on disk.</li>
<li>Run a benchmark using the <code>bench ../imdb/queries outputfile.txt</code> command in the SkinnerDB console (you may need to adapt the relative path to the directory containing benchmark queries, replace <code>outputfile.txt</code> by a file name of your choosing).</li>
</ol>

After running the benchmark, benchmark results can be found in the specified output file. Benchmark results include per-query times for each of the processing phases (pre-processing, join phase, and post-processing) as well as many other statistics such as the number of tuples generated (column "Tuples") or the number of UCT tree nodes generated (column "NrUctNodes").

Typical times for running all queries of the join order benchmark are 83 seconds with indexes (104 seconds without indexes) on our benchmarking platform (Dell PowerEdge R640 server with 2 Intel Xeon 2.3 GHz CPUs and 256 GB of RAM running OpenJDK 1.8). Times may slightly vary from run to run as join ordering decisions depend initially on randomization.

# Quickstart

0. Compile the code (run `mvn package`).

1. Create a new database using target/CreateDB.jar or by executing tools/CreateDB.java. You need to specify two command line parameters: the database name and an (existing) directory in which the corresponding data is stored.

2. Start the Skinner console. The Skinner console can be accessed via target/Skinner.jar or by executing console/SkinnerCmd.java. You need to specify the database directory as command line parameter (the same directory that was specified in the call to CreateDB.jar).

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

SkinnerDB is developed by the Cornell database group (http://www.cs.cornell.edu/database/).
