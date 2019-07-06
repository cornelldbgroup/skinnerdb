# skinnerdb

This repository contains a very early version of a re-implementation of SkinnerDB, 
described in the paper "SkinnerDB: Regret-bounded query evaluation via reinforcement learning" at SIGMOD 2019. 

This source code is currently under development and ** NOT CONSIDERED STABLE ***. 

We expect to release the first stable version in the next months.

# Quickstart

1. Create a new database using jars/CreateDB.jar or by executing tools/CreateDB.java. You need to specify two command line parameters: the database name and an (existing) directory in which the corresponding data is stored.

2. Start the Skinner console. The Skinner console can be accessed via jars/Skinner.jar or by executing console/SkinnerCmd.java. You need to specify the database directory as command line parameter (the same directory that was specified in the call to CreateDB.jar).

SkinnerDB is specialized for in-memory processing. The default settings for java JVM heap space etc. are in general insufficient. When starting the Skinner console, make sure to specify a sufficient amount of main memory using the -Xmx (and -Xms) parameters. Specifying more memory than necessary can improve performance further as it reduces the need for garbage collection. Also, to obtain more stable per-query performance, consider replacing the default garbage collector by specifying -XX:+UseConcMarkSweepGC as JVM parameter. Our recommendations are based on our experiences with our current test hardware and may not generalize to all platforms.

3. Create the database schema. SkinnerDB currently supports a limited number of SQL data types (text, int, and double). The example script located under imdb/skinner.schema.sql demonstrates how to create the schema of the join order benchmark. Note that you can execute commands in files via the 'exec <path>' command from the Skinner console. Run 'help' in the console to obtain a complete list of utility commands.

4. Load the data. SkinnerDB currently supports loading table data from CSV files. Run the command 'exec <table name> <path to .csv file> <representation of NULL values>' in the Skinner console to load data from the corresponding file into the specified table. The example script under 'imdb/skinner.load.sql' shows commands by which data for the join order benchmark can be loaded (assuming .csv files at the specified locations). The final command in that file refers to the next point.

5. (Optional) Compress string values after loading all data for all tables. Run the 'compress' command to create a dictionary that maps strings that appear in the database to integer code values. Processing integer values is significantly more efficient than processing strings. Compression may take a while as it iterates over the entire database. This pre-processing overhead may however pay off at run time.

6. (Optional) Create indices for the database columns. Run the 'index all' command in the Skinner console to create indices on all database columns. Again, this may take a while but can pay off at run time. Currently, we do not store indices on hard disk. This means that the 'index all' command (as opposed to the 'compress' command!) has to be re-run each time after starting the Skinner console.

7. Run analytical SQL queries. The current prototype only supports a very limited subset of SQL and not all features have been tested yet. The current support includes (without guarantees) select queries with inequality and equality predicates, LIKE expressions (as they appear in the join order benchmark, some special cases are currently not handled correctly), logical and arithmetic expressions, minimum and maximum aggregation, joins with predicates specified in the SQL WHERE clause, grouping, and sorting.

# Team

SkinnerDB is developed by the Cornell database group (http://www.cs.cornell.edu/database/).
