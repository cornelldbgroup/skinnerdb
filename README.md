# SkinnerMT

This directory contains a prototype of parallel database which applies three different parallel algorithms to speedup query optimization and execution. The prototype needs to load data from the disk into main memory which might takes several minutes. We expect to implement a Client-Server DBMS architecture to avoid redundant loading time.

# Running Benchmarks

In SkinnerMT, we provided three benchmarks. The <a href="http://www.vldb.org/pvldb/vol9/p204-leis.pdf">join order benchmark</a> is a popular benchmark for query optimizers. The <a href="http://www.tpc.org/tpch/">TPC-H</a> (scaling factor of 10) is easy to optimized due to uniform data. The <a href="https://doi.org/10.1007/978-3-319-72401-0_8">JCC-H</a> (scaling factor of 10) is difficult due to skewed data. Execute the following steps to run SkinnerMT on those benchmarks:

<ol>
<li>Download the database in the SkinnerMT format <a href="https://drive.google.com/drive/folders/1QwLJGys31Dp9iUhnTK78q3fPvQh6_-B5?usp=sharing">databases.zip</a>. Decompress the linked .zip file. Then download and decompress source codes and executable jar files <a href="https://drive.google.com/drive/folders/1QwLJGys31Dp9iUhnTK78q3fPvQh6_-B5?usp=sharing">skinnermt.zip</a>.</li>
<li>Start SkinnerMT using the bash script <code>Skinner.sh</code>. For Linux, use the following command to initialize SkinnerMT (while replacing /path/to/skinner/data by the path to the decompressed database and nr_threads by the number of running threads): 
<p>
<code>
./Skinner.sh /path/to/skinner/data nr_threads
</code>
For example,
<code>
./Skinner.sh ./imdb 30
</code>

The setting for heap space is 100 GB (-Xmx100G) for our benchmarking platform in default to avoid potential garbage collection overhead. Running under less heap space (50 GB at the minimum) can save main memory but as a trade-off it may loss a few seconds of end-to-end performance.
</p> 
</li>
<li>Run a benchmark using the bench command in the SkinnerMT console. For example, <code>bench ./imdb/queries outputfile.txt</code> command will benchmark queries in ./imdb/queries directory and write experimental results into outputfile.txt. You may need to adapt the relative path to the directory containing benchmark queries, replace <code>outputfile.txt</code> by a file name of your choosing.</li>
</ol>

# Output
After running the benchmark, benchmark results can be found in the specified output file. Benchmark results include per-query times for each of the processing phases (pre-processing, join phase, and post-processing) as well as many other statistics such as the number of tuples generated (column "Tuples") or the memory consumption. Some columns related to performance and memory consumption:

<ol>
<li>Query: name of query to process</li>
<li>IsWarmup: end-to-end performance of the running query</li>
<li>Millis: end-to-end performance of the running query</li>
<li>PreMillis: time of pre-processing phase</li>
<li>JoinMillis: time of join phase</li>
<li>MatMillis: time of materialization</li>
<li>PostMillis: time of post-processing phase</li>
<li>FilterMillis: time of filtering in pre-processing phase</li>
<li>IndexMillis: time of index creation in pre-processing phase</li>
<li>GroupByMillis: time of groupby in post-processing phase</li>
<li>AggregateMillis: time of aggregation in post-processing phase</li>
<li>OrderMillis: time of ordering in post-processing phase</li>
<li>Tuples: number of partial or completed tuples considered during the join phase</li>
<li>Samples: number of learning samples during the join phase</li>
<li>Lookups: number of index lookups during the join phase (<b>implemented only for sequential version</b>)</li>
<li>NrIndexEntries: sum of index entries for the values used in index lookups (<b>implemented only for sequential version</b>)</li>
<li>nrUniqueLookups: number of index lookups where the number of corresponding entries is at most one (<b>implemented only for sequential version</b>)</li>
<li>NrPlans: number of query plans tried during the join phase (<b>implemented only for sequential version</b>)</li>
<li>JoinCard: join result cardinality of last processed sub-query</li>
<li>AvgReward: average reward obtained during the join phase (<b>implemented only for sequential version</b>)</li>
<li>MaxReward: maximum reward obtained during the join phase (<b>implemented only for sequential version</b>)</li>
<li>TotalWork: total work (including redundant work) that is calculated based on table offsets after query evaluation (<b>implemented only for sequential version</b>)</li>
<li>DataSize: memory consumption of relations, columns and indexes</li>
<li>UctSize: memory consumption of uct trees</li>
<li>StateSize: memory consumption of progress tracker tree</li>
<li>JoinSize: memory consumption of data structures used in the join phase</li>
</ol>


# Configuration

SkinnerMT includes parameters for specific benchmarks and data sets. Those parameters are set to some default values. You can find these parameters in config.sdb under the database directory. The configuration file includes:
<ol>
<li>THREADS: number of available threads for SkinnerMT. In default, it is the number of available processors in the running machine</li>
<li>NR_WARMUP: number of warm-up runs before the actual run</li>
<li>NR_EXECUTORS: number of executors for task parallel</li>
<li>NR_BATCHES: number of batches for task parallel</li>
<li>PARALLEL_ALGO: parallel algorithms used in the join phase. 
    <ul>
        <li>Data Parallel: DP</li>
        <li>Search Parallel: SP-O, SP-P, SP-H, SP-C, Root, Leaf, Tree</li>
        <li>Task Parallel: TP</li>
    </ul>
</li>
<li>WRITE_RESULTS: whether to write results of queries into a file for the 'bench' command. The output file is named by outputfile.txt.res </li>
</ol>

# Create databases (Optional)

1. Create a new database using jars/CreateDB.jar or by executing tools/CreateDB.java. You need to specify two command line parameters: the database name and an (existing) directory in which the corresponding data is stored.

2. Start the Skinner console. The Skinner console can be accessed via jars/Skinner.jar or by executing console/SkinnerCmd.java. You need to specify the database directory as command line parameter (the same directory that was specified in the call to CreateDB.jar).

3. Create the database schema. SkinnerDB currently supports a limited number of SQL data types (text, int, and double). The example script located under imdb/skinner.schema.sql demonstrates how to create the schema of the join order benchmark. Note that you can execute commands in files via the 'exec <path>' command from the Skinner console. Run 'help' in the console to obtain a complete list of utility commands.

4. Load the data. SkinnerMT currently supports loading table data from CSV files. Run the command 'exec \<table name\> \<separator\> \<path to .csv file\> \<representation of NULL values\>' in the Skinner console to load data from the corresponding file into the specified table. The example script under 'imdb/skinner.load.sql' shows commands by which data for the join order benchmark can be loaded (assuming .csv files at the specified locations). The final command in that file refers to the next point.

5. (Optional) Compress string values after loading all data for all tables. Run the 'compress' command to create a dictionary that maps strings that appear in the database to integer code values. Processing integer values is significantly more efficient than processing strings. Compression may take a while as it iterates over the entire database. This pre-processing overhead may however pay off at run time.

6. Restart SkinnerDB (leave the console by entering 'quit' ).

7. (Optional) Create indices for the database columns. Run the 'index all' command in the Skinner console to create indices on all database columns. Again, this may take a while but can pay off at run time. Currently, we do not store indices on hard disk. This means that the 'index all' command (as opposed to the 'compress' command!) has to be re-run each time after starting the Skinner console.

8. Run analytical SQL queries. The current prototype only supports a very limited subset of SQL and not all features have been tested yet. The current support includes (without guarantees) select queries with inequality and equality predicates, LIKE expressions (as they appear in the join order benchmark, some special cases are currently not handled correctly), logical and arithmetic expressions, minimum and maximum aggregation, joins with predicates specified in the SQL WHERE clause, grouping, and sorting.
