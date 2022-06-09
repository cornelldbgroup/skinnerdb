# SkinnerMT



This directory contains a prototype of a parallel database system using intra-query learning. SkinnerMT applies three different parallel algorithms to speedup query optimization and execution. 
The prototype needs to load data from the disk into main memory which might takes several minutes. We expect to implement a Client-Server DBMS architecture to avoid redundant loading time.


# Requirements
- Ubuntu with Java ≥ 15.
- gcc and g++ ≥ 9.4.0
- libcuckoo

We have verified that the SkinnerMT can be built successfully on Amazon EC2 c4.8xlarge with Ubuntu 22.04 LTS and Java 17.0.1 installed.


# Running Benchmarks

In SkinnerMT, we provided three benchmarks. The <a href="http://www.vldb.org/pvldb/vol9/p204-leis.pdf">join order benchmark</a> is a popular benchmark for query optimizers. The <a href="http://www.tpc.org/tpch/">TPC-H benchmark</a> (scaling factor of 10) is easy to optimize due to uniform data. The <a href="https://doi.org/10.1007/978-3-319-72401-0_8">JCC-H benchmark</a> (scaling factor of 10) is difficult due to skewed data. Execute the following steps to run SkinnerMT on those benchmarks:

<ol>
<li>Download and decompress source codes and executable jar files <a href="https://drive.google.com/file/d/1CU0sJlR-GvSBKzzfJO-CyaFlM_PmN4Tb/view?usp=sharing">skinnermt.zip</a>. 
Then download the database in the SkinnerMT format <a href="https://drive.google.com/file/d/1zr9pKMfK33IOlZ26YrvLpO1STi7Rzueu/view?usp=sharing">databases.zip</a>. Decompress the linked .zip file under <code>skinnermt</code>.</li>

You can use the gdown tool to download files from Google Drive via the Linux console.
```
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3.9
sudo apt install python3-pip
pip install gdown
sudo pip install --upgrade gdown
gdown 1RvwNLveDfiRPz7sTfpPX7_zQb77rRgHE
gdown 1zr9pKMfK33IOlZ26YrvLpO1STi7Rzueu
sudo apt install unzip
unzip skinnermt.zip
unzip databases.zip
mv imdb skinnermt
mv tpch-sf-10 skinnermt
mv jcch-sf-10 skinnermt
```

The final directory structure should be:

```
skinnermt
¦   README.md
¦
+---Filter
¦
+---src  
¦
+---scripts
¦   ¦   skinnerexp.sh
¦   ¦   Skinner.sh
¦   ¦   ...
¦
¦
+---imdb
¦   ¦   data
¦   ¦   config.sdb
¦   ¦   ...
¦
¦   
+---tpch-sf-10
¦   ¦   data
¦   ¦   config.sdb
¦   ¦   ...
¦
¦
+---tpch-sf-10
¦   ¦   data
¦   ¦   config.sdb
¦   ¦   ...
```
<li>Install libcuckoo library based on the <a href="https://github.com/efficient/libcuckoo">Github instructions</a>. </li>

```

sudo apt-get update && sudo apt-get install cmake
mkdir build
cd build
git clone https://github.com/efficient/libcuckoo
cd libcuckoo
cmake -DCMAKE_INSTALL_PREFIX=../install -DBUILD_EXAMPLES=1 -DBUILD_TESTS=1 .
make all
make install
cd ~/skinnermt/

```

<li>Compile JNI codes under the <code>Filter</code> directory: </li>

```
cd Filter
g++ -std=c++11 -lpthread -shared -fPIC -O3 jniFilter.cpp -o jniFilter.so -I/home/ubuntu/build/install/include/
```


**Note: The script will generate the JNI library file <code>jniFilter.so</code>. 
Please replace the variable <code>JNI_PATH</code> (in <code>config.sdb</code> for each database) by the path to <code>jniFilter.so</code>.**

<li>Start SkinnerMT using the bash script <code>Skinner.sh</code>. For Linux, use the following command to initialize SkinnerMT (while replacing /path/to/skinner/data by the path to the decompressed database and nr_threads by the number of running threads): 
<p>

```
cd ~/skinnermt/scripts/
./Skinner.sh /path/to/skinner/data nr_threads
```

The script will run corresponding .jar file that can be invoked directly on different platforms. We recommend setting a high value for Java heap space (parameter Xmx) to minimize garbage collection overheads (the current SkinnerMT version isn't optimized for main memory footprint).
</p> 
</li>
<li>Run a benchmark using the bench command in the SkinnerMT console. Queries for each database can be found under the according directory. For example, <code>bench ../imdb/queries outputfile.txt</code> command will benchmark queries in ../imdb/queries directory and write experimental results into outputfile.txt. You may need to adapt the relative path to the directory containing benchmark queries, replace <code>outputfile.txt</code> by a file name of your choosing.
</li>

<li>
Alternatively, we provide a bash script <code>./scripts/skinnerexp.sh</code> to run performance
experiments automatically.
<p>

```
./skinnerexp.sh [nr_runs] [benchmark] [algorithm]
```

The script will run queries of <code>[benchmark]</code>(e.g. imdb/tpch/jcch) <code>[nr_runs]</code> (e.g. 10)
times by using <code>[algorithm]</code> parallelization (e.g. dp/sp/hp).
</p> 
</li>

For reproducing the experiments in the paper, we recommend using the
Epsilon GC (<code>-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC -XX:+AlwaysPreTouch</code>) 
and larger heap space (<code>-Xmx200G</code>). 

</ol>

# Output
After running the benchmark, benchmark results can be found in the specified output file. Benchmark results include per-query times for each of the processing phases (pre-processing, join phase, and post-processing) as well as many other statistics such as the number of tuples generated (column "Tuples") or the memory consumption:

<ol>
<li>Query: name of query to process</li>
<li>IsWarmup: boolean flag to present whether the run is warmup or not</li>
<li>Millis: end-to-end performance of the running query</li>
<li>PreMillis: time of pre-processing phase</li>
<li>JoinMillis: time of join phase</li>
<li>MatMillis: time of materialization</li>
<li>PostMillis: time of post-processing phase</li>
<li>FilterMillis: time of filtering in the pre-processing phase</li>
<li>IndexMillis: time of index creation in the pre-processing phase</li>
<li>GroupByMillis: time of groupby in the post-processing phase</li>
<li>AggregateMillis: time of aggregation in the post-processing phase</li>
<li>OrderMillis: time of ordering in the post-processing phase</li>
<li>Tuples: number of partial or completed tuples considered during the join phase</li>
<li>Samples: number of learning samples during the join phase</li>
<li>Lookups: number of index lookups during the join phase (<b>implemented only for sequential version</b>)</li>
<li>NrIndexEntries: sum of index entries for the values used in index lookups (<b>implemented only for sequential version</b>)</li>
<li>nrUniqueLookups: number of index lookups where the number of corresponding entries is at most one (<b>implemented only for sequential version</b>)</li>
<li>NrPlans: number of query plans tried during the join phase (<b>implemented only for sequential version</b>)</li>
<li>JoinCard: join result cardinality of last processed sub-query (for queries that only return single result row (e.g. MIN or MAX operators on selected columns), post-processing is directly conducted whenever completed result tuples are found, making the JoinCard always less or equal 1.)</li>
<li>AvgReward: average reward obtained during the join phase (<b>implemented only for sequential version</b>)</li>
<li>MaxReward: maximum reward obtained during the join phase (<b>implemented only for sequential version</b>)</li>
<li>TotalWork: total work (including redundant work) that is calculated based on table offsets after query evaluation (<b>implemented only for sequential version</b>)</li>
<li>DataSize: memory consumption of relations, columns and indexes (<b>works when TEST_MEM is set to true</b>)</li>
<li>UctSize: memory consumption of uct trees (<b>works when TEST_MEM is set to true</b>)</li>
<li>StateSize: memory consumption of progress tracker tree (<b>works when TEST_MEM is set to true</b>)</li>
<li>JoinSize: memory consumption of data structures used in the join phase (<b>works when TEST_MEM is set to true</b>)</li>
</ol>


# Configuration

SkinnerMT includes parameters for specific benchmarks and data sets. Those parameters are set to some default values. You can find these parameters in config.sdb under the database directory. The configuration file includes:
<ol>
<li>THREADS: number of available threads for SkinnerMT. By default, it is the number of available processors in the running machine</li>
<li>NR_WARMUP: number of warm-up runs before the actual run</li>
<li>PARALLEL_ALGO: parallel algorithms used in the join phase. 
    <ul>
        <li>Data Parallel: DP</li>
        <li>Search Parallel: SP</li>
        <li>Hybrid Parallel: HP</li>
    </ul>
</li>
<li>TEST_MEM: whether to measure memory consumption including base tables, indexes, uct tree, progress tracker and auxiliary data structures (Note that open this flag may add overhead of measuring memory consumption)</li>
<li>WRITE_RESULTS: whether to write results of queries into a file for the 'bench' command. The output file is named by outputfile.txt.res </li>
<li>JNI_PATH: absolute path to JNI libraries.</li>
</ol>

# Create databases (Optional)

1. Create a new database using jars/CreateDB.jar or by executing tools/CreateDB.java. You need to specify two command line parameters: the database name and an (existing) directory in which the corresponding data is stored.

2. Start the Skinner console. The Skinner console can be accessed via jars/Skinner.jar or by executing console/SkinnerCmd.java. You need to specify the database directory as command line parameter (the same directory that was specified in the call to CreateDB.jar).

3. Create the database schema. SkinnerMT currently supports a limited number of SQL data types (text, int, and double). The example script located under imdb/skinner.schema.sql demonstrates how to create the schema of the join order benchmark. Note that you can execute commands in files via the 'exec <path>' command from the Skinner console. Run 'help' in the console to obtain a complete list of utility commands.

4. Load the data. SkinnerMT currently supports loading table data from CSV files. Run the command 'exec \<table name\> \<separator\> \<path to .csv file\> \<representation of NULL values\>' in the Skinner console to load data from the corresponding file into the specified table. The example script under 'imdb/skinner.load.sql' shows commands by which data for the join order benchmark can be loaded (assuming .csv files at the specified locations). The final command in that file refers to the next point.

5. (Optional) Compress string values after loading all data for all tables. Run the 'compress' command to create a dictionary that maps strings that appear in the database to integer code values. Processing integer values is significantly more efficient than processing strings. Compression may take a while as it iterates over the entire database. This pre-processing overhead may however pay off at run time.

6. Restart SkinnerMT (leave the console by entering 'quit' ).

7. (Optional) Create indices for the database columns. Run the 'index all' command in the Skinner console to create indices on all database columns. Again, this may take a while but can pay off at run time. Currently, we do not store indices on hard disk. This means that the 'index all' command (as opposed to the 'compress' command!) has to be re-run each time after starting the Skinner console.

8. Run analytical SQL queries. The current prototype only supports a very limited subset of SQL and not all features have been tested yet. The current support includes (without guarantees) select queries with inequality and equality predicates, LIKE expressions (as they appear in the join order benchmark, some special cases are currently not handled correctly), logical and arithmetic expressions, minimum and maximum aggregation, joins with predicates specified in the SQL WHERE clause, grouping, and sorting.
