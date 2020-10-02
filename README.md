# SkinnerMT

This directory contains a parallel version based on re-implementation of SkinnerDB, 
described in the paper <a href="https://dl.acm.org/citation.cfm?id=3275600">SkinnerDB: Regret-bounded query evaluation via reinforcement learning</a> at SIGMOD 2019 (see video recording of SIGMOD talk <a href="https://www.youtube.com/watch?v=QRYVnKaZ9fw">here</a>). 

# Quickstart

1. Create a new database using jars/CreateDB.jar or by executing tools/CreateDB.java. You need to specify two command line parameters: the database name and an (existing) directory in which the corresponding data is stored.

2. Start the Skinner console. The Skinner console can be accessed via jars/Skinner.jar or by executing console/SkinnerCmd.java. You need to specify the database directory as command line parameter (the same directory that was specified in the call to CreateDB.jar).

3. Create the database schema. SkinnerDB currently supports a limited number of SQL data types (text, int, and double). The example script located under imdb/skinner.schema.sql demonstrates how to create the schema of the join order benchmark. Note that you can execute commands in files via the 'exec <path>' command from the Skinner console. Run 'help' in the console to obtain a complete list of utility commands.

4. Load the data. SkinnerMT currently supports loading table data from CSV files. Run the command 'exec \<table name\> \<separator\> \<path to .csv file\> \<representation of NULL values\>' in the Skinner console to load data from the corresponding file into the specified table. The example script under 'imdb/skinner.load.sql' shows commands by which data for the join order benchmark can be loaded (assuming .csv files at the specified locations). The final command in that file refers to the next point.

5. (Optional) Compress string values after loading all data for all tables. Run the 'compress' command to create a dictionary that maps strings that appear in the database to integer code values. Processing integer values is significantly more efficient than processing strings. Compression may take a while as it iterates over the entire database. This pre-processing overhead may however pay off at run time.

6. Restart SkinnerDB (leave the console by entering 'quit' ).

7. (Optional) Create indices for the database columns. Run the 'index all' command in the Skinner console to create indices on all database columns. Again, this may take a while but can pay off at run time. Currently, we do not store indices on hard disk. This means that the 'index all' command (as opposed to the 'compress' command!) has to be re-run each time after starting the Skinner console.

8. Run analytical SQL queries. The current prototype only supports a very limited subset of SQL and not all features have been tested yet. The current support includes (without guarantees) select queries with inequality and equality predicates, LIKE expressions (as they appear in the join order benchmark, some special cases are currently not handled correctly), logical and arithmetic expressions, minimum and maximum aggregation, joins with predicates specified in the SQL WHERE clause, grouping, and sorting.


# Running Benchmarks

In SkinnerMT, we provided three benchmarks. The <a href="http://www.vldb.org/pvldb/vol9/p204-leis.pdf">join order benchmark</a> is a popular benchmark for query optimizers. The <a href="http://www.tpc.org/tpch/">TPC-H</a> is easy to optimized due to uniform data. The <a href="https://doi.org/10.1007/978-3-319-72401-0_8">JCC-H</a> is difficult due to slightly skewed data. Execute the following steps to run SkinnerMT on those benchmarks:

<ol>
<li>Download the database in the SkinnerMT format <a href="https://drive.google.com/file/d/1UCXtiPvVlwzUCWxKM6ic-XqIryk4OTgE/view?usp=sharing">here</a>. Decompress the linked .zip file.</li>
<li>Start SkinnerDB using the executable .jar file in the jars sub-folder. For Linux, use the following command in the jars directory (while replacing /path/to/skinner/data by the path to the decompressed database): 
<p>
<code>
java -jar -Xmx100G Skinner.jar /path/to/skinner/data
</code>
</p>
The settings for heap space (<code>-Xmx100G</code>) work best for our benchmarking platform but may need to be revised for different machines.    
</li>
<li>Run a benchmark using the <code>bench ../imdb/queries outputfile.txt</code> command in the SkinnerMT console (you may need to adapt the relative path to the directory containing benchmark queries, replace <code>outputfile.txt</code> by a file name of your choosing).</li>
</ol>

# Output
After running the benchmark, benchmark results can be found in the specified output file. Benchmark results include per-query times for each of the processing phases (pre-processing, join phase, and post-processing) as well as many other statistics such as the number of tuples generated (column "Tuples") or the memory consumption. Some columns related to performance and memory consumption:

<ol>
<li>Query: name of query to process</li>
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
<li>DataSize: memory consumption of relations, columns and indexes</li>
<li>UctSize: memory consumption of uct trees</li>
<li>StateSize: memory consumption of progress tracker tree</li>
<li>JoinSize: memory consumption of data structures initialized during the join phase</li>
</ol>


# Configuration

SkinnerMT includes parameters for specific benchmarks and data sets. Those parameters are set to some default values. You can find these parameters in skinner.config.sdb under the database directory. The configuration file includes:
<ol>
<li>THREADS: number of available threads for SkinnerMT</li>
<li>WARMUP: number of warm-up runs before the actual run</li>
<li>NRBATCHES: number of batches for task parallel</li>
<li>PARALLEL_SPEC: parallel algorithm used in the join phase</li>
<li>TODO:more configuration parameters</li>
</ol>

