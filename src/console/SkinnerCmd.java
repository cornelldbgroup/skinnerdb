package console;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import benchmark.BenchUtil;
import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import compression.Compressor;
import config.*;
import data.ColumnData;
import ddl.TableCreator;
import diskio.LoadCSV;
import diskio.PathUtil;
import execution.Master;
import indexing.Indexer;
import joining.parallel.threads.ThreadPool;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import print.RelationPrinter;
import query.ColumnRef;
import query.SQLexception;
import statistics.QueryStats;
import tools.Configuration;

/**
 * Runs Skinner command line console.
 *
 * @author Anonymous
 */
public class SkinnerCmd {
    /**
     * Path to database directory.
     */
    static String dbDir;

    /**
     * Checks whether file exists and displays
     * error message if not. Returns true iff
     * the file exists.
     *
     * @param filePath check for file at that location
     * @return true iff the file exists
     */
    static boolean fileOrError(String filePath) {
        if ((new File(filePath)).exists()) {
            return true;
        } else {
            System.out.println("Error - input file at " +
                    filePath + " does not exist");
            return false;
        }
    }

    /**
     * Processes a command for benchmarking all queries in a
     * given directory.
     *
     * @param input input command
     * @throws Exception
     */
    static void processBenchCmd(String input) throws Exception {
        String[] inputFrags = input.split("\\s");
        if (inputFrags.length != 3) {
            System.out.println("Error - specify only path "
                    + "to directory containing queries and "
                    + "name of output file");
        } else {
            // Check whether directory exists
            String dirPath = inputFrags[1];
            if (fileOrError(dirPath)) {
                // Open benchmark result file and write header
                String outputName = inputFrags[2];
                PrintWriter benchOut = new PrintWriter(outputName);
                BenchUtil.writeBenchHeader(benchOut);
                boolean writeResult = GeneralConfig.WRITE_RESULTS;
                PrintWriter queryOut = null;
                if (writeResult) {
                    queryOut = new PrintWriter(outputName + ".res");
                }
                // Load all queries to benchmark
                Map<String, Statement> nameToQuery =
                        BenchUtil.readAllQueries(dirPath);
                // Read optimal join orders
                String optimalFile = Configuration.getProperty("OPTIMAL");
                Map<String, int[]> nameToOptimal = BenchUtil.readOptimalJoinOrders(optimalFile);

                // Iterate over queries
                for (Entry<String, Statement> entry : nameToQuery.entrySet()) {
                    String queryName = entry.getKey();
                    Statement query = entry.getValue();
                    System.out.println(queryName);
                    System.out.println(query.toString());
                    QueryStats.queryName = queryName;
                    QueryStats.optimal = nameToOptimal.get(queryName);
                    processSQL(query.toString(), benchOut, queryOut);
                }
                // Close benchmark result file
                benchOut.close();
                if (queryOut != null) {
                    queryOut.close();
                }
            }
        }
    }

    /**
     * Processes a command for loading data from a CSV file on disk.
     *
     * @param input input command
     * @throws Exception
     */
    static void processLoadCmd(String input) throws Exception {
        // Load data from file into table
        String[] inputFrags = input.split("\\s");
        if (inputFrags.length != 5) {
            System.out.println("Error - specify table name, "
                    + "path to .csv file, separator, and null "
                    + "value representation, "
                    + "separated by spaces.");
        } else {
            // Retrieve schema information on table
            String tableName = inputFrags[1];
            TableInfo table = CatalogManager.
                    currentDB.nameToTable.get(tableName);
            // Does the table exist?
            if (table == null) {
                System.out.println("Error - cannot find table " + tableName);
            } else {
                String csvPath = inputFrags[2];
                // Does input path exist?
                if (fileOrError(csvPath)) {
                    String separatorStr = inputFrags[3];
                    if (separatorStr.length() != 1) {
                        System.out.println("Inadmissible separator: " +
                                separatorStr + " (requires one character)");
                    } else {
                        char separator = separatorStr.charAt(0);
                        String nullRepresentation = inputFrags[4];
                        LoadCSV.load(csvPath, table,
                                separator, nullRepresentation);
                    }
                }
            }
        }
    }

    /**
     * Processes SQL commands in specified file.
     *
     * @param input input string for script command
     * @throws Exception
     */
    static void processFile(String input) throws Exception {
        // Check whether right parameters specified
        String[] inputFrags = input.split("\\s");
        if (inputFrags.length != 2) {
            System.err.println("Error - specify script path");
        } else {
            String path = inputFrags[1];
            // Verify whether input file exists
            if (fileOrError(path)) {
                Scanner scanner = new Scanner(new File(path));
                scanner.useDelimiter(Pattern.compile(";"));
                while (scanner.hasNext()) {
                    String sqlCmd = scanner.next().trim();
                    try {
                        System.out.println("Processing statement '" + sqlCmd + "'");
                        processInput(sqlCmd);
                    } catch (Exception e) {
                        System.err.println("Error processing command " + sqlCmd);
                        e.printStackTrace();
                    }
                }
                scanner.close();
            }
        }
    }

    /**
     * Process input string as SQL statement.
     *
     * @param input         input text
     * @param benchOut	    writer to benchmark result file
     * @param queryOut	    writer to query result file
     *
     * @throws Exception
     */
    static void processSQL(String input, PrintWriter benchOut, PrintWriter queryOut) throws Exception {
        // Try parsing as SQL query
        Statement sqlStatement = null;
        String name = QueryStats.queryName;
        try {
            sqlStatement = CCJSqlParserUtil.parse(input);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error in parsing SQL command");
            return;
        }
        // Distinguish statement type
        if (sqlStatement instanceof CreateTable) {
            TableInfo table = TableCreator.addTable(
                    (CreateTable) sqlStatement);
            CatalogManager.currentDB.storeDB();
            System.out.println("Created " + table.toString());
        } else if (sqlStatement instanceof CreateView) {
            CreateView createView = (CreateView) sqlStatement;
            List<String> columnNames = createView.getColumnNames();
            PlainSelect plainSelect = (PlainSelect) createView.getSelectBody();
            Table view = createView.getView();
            try {
                for (int warmupCtr = 0; warmupCtr < GeneralConfig.NR_WARMUP; warmupCtr++) {
                    GeneralConfig.ISTESTCASE = false;
                    Master.executeSelect(plainSelect,
                            false, -1, -1, null, name, benchOut);
                    BufferManager.unloadTempData();
                    CatalogManager.removeTempTables();
                    sqlStatement = CCJSqlParserUtil.parse(input);
                    plainSelect = (PlainSelect) ((CreateView) sqlStatement).getSelectBody();
                }
                GeneralConfig.ISTESTCASE = true;
                Master.executeSelect(plainSelect,
                        false, -1, -1, null, name, benchOut);

            } catch (SQLexception e) {
                System.out.println(e.getMessage());
            } catch (Exception e) {
                throw e;
            } finally {
                CreateTable createTable = new CreateTable();
                createTable.setTable(view);
                List<ColumnDefinition> definitions = new ArrayList<>();
                TableInfo tableInfo = CatalogManager.getTable(NamingConfig.FINAL_RESULT_NAME);
                for (int i = 0; i < columnNames.size(); i++) {
                    String columnName = columnNames.get(i);
                    ColumnDefinition columnDefinition = new ColumnDefinition();
                    columnDefinition.setColumnName(columnName);
                    ColDataType colDataType = new ColDataType();
                    String resultColumn = tableInfo.columnNames.get(i);
                    String resultType = tableInfo.nameToCol.get(resultColumn).type.toString();
                    colDataType.setDataType(resultType);
                    columnDefinition.setColDataType(colDataType);
                    definitions.add(columnDefinition);
                }
                createTable.setColumnDefinitions(definitions);
                TableInfo table = TableCreator.addTable(createTable);
                CatalogManager.currentDB.storeDB();
                System.out.println("Created " + table.toString());
                for (int i = 0; i < columnNames.size(); i++) {
                    String columnName = columnNames.get(i);
                    String resultColumn = tableInfo.columnNames.get(i);
                    ColumnInfo columnInfo = tableInfo.nameToCol.get(resultColumn);
                    ColumnRef columnRef = new ColumnRef(tableInfo.name, columnInfo.name);
                    ColumnData resultData = BufferManager.getData(columnRef);
                    ColumnRef newColumnRef = new ColumnRef(table.name, columnName);
                    BufferManager.colToData.put(newColumnRef, resultData);
                }
                CatalogManager.updateStats(table.name);
                // Clean up intermediate results
                BufferManager.unloadTempData();
                CatalogManager.removeTempTables();
            }
        } else if (sqlStatement instanceof Drop) {
            Drop drop = (Drop) sqlStatement;
            String tableName = drop.getName().getName();
            // Verify that table to drop exists
            if (!CatalogManager.currentDB.nameToTable.containsKey(tableName)) {
                throw new SQLexception("Error - table " +
                        tableName + " does not exist");
            }
            CatalogManager.currentDB.nameToTable.remove(tableName);
            CatalogManager.currentDB.storeDB();
            System.out.println("Dropped " + tableName);

        } else if (sqlStatement instanceof Select) {
            Select select = (Select) sqlStatement;
            if (select.getSelectBody() instanceof PlainSelect) {
                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                boolean printResult = plainSelect.getIntoTables() == null;
                BufferManager.unloadCache(name.charAt(0) + "" + name.charAt(1));
                try {
                    for (int warmupCtr = 0; warmupCtr < GeneralConfig.NR_WARMUP; warmupCtr++) {
                        GeneralConfig.ISTESTCASE = false;
                        Master.executeSelect(plainSelect,
                                false, -1, -1, null, name, benchOut);
                        BufferManager.unloadTempData();
                        CatalogManager.removeTempTables();
                        sqlStatement = CCJSqlParserUtil.parse(input);
                        select = (Select) sqlStatement;
                        plainSelect = (PlainSelect) select.getSelectBody();
                    }
                    GeneralConfig.ISTESTCASE = true;
                    // Run the query after the warm up.
                    Master.executeSelect(plainSelect, false,
                            -1, -1, null, name, benchOut);

                    // Display query result if no target tables specified
                    // and if this is not a benchmark run.
                    if (benchOut == null && printResult) {
                        // Display on console
                        RelationPrinter.print(
                                NamingConfig.FINAL_RESULT_NAME);
                    }
                    if (queryOut != null) {
                        RelationPrinter.write(NamingConfig.FINAL_RESULT_NAME, queryOut);
                    }
                } catch (SQLexception e) {
                    System.out.println(e.getMessage());
                } catch (Exception e) {
                    throw e;
                } finally {
                    // Clean up intermediate results
                    BufferManager.unloadTempData();
                    CatalogManager.removeTempTables();
                }
            } else {
                System.out.println("Only plain select statements supported");
            }
        } else {
            System.out.println("Statement type " +
                    sqlStatement.getClass().toString() +
                    " not supported!");
        }
    }

    /**
     * Processes an explain statement.
     *
     * @param inputFrags fragments of user input - should be explain
     *                   keyword, plot directory, plot bound, and plot
     *                   frequency, followed by query fragments.
     * @throws Exception
     */
    static void processExplain(String[] inputFrags) throws Exception {
        String plotDir = inputFrags[1];
        String name = QueryStats.queryName;
        if (fileOrError(plotDir)) {
            int plotAtMost = Integer.parseInt(inputFrags[2]);
            int plotEvery = Integer.parseInt(inputFrags[3]);
            // Try parsing as SQL query
            StringBuilder sqlBuilder = new StringBuilder();
            int nrFragments = inputFrags.length;
            for (int fragCtr = 4; fragCtr < nrFragments; ++fragCtr) {
                sqlBuilder.append(inputFrags[fragCtr]);
                sqlBuilder.append(" ");
            }
            Statement sqlStatement = null;
            try {
                sqlStatement = CCJSqlParserUtil.parse(sqlBuilder.toString());
            } catch (Exception e) {
                System.out.println("Error in parsing SQL command");
                return;
            }
            // Execute explain command
            if (sqlStatement instanceof Select) {
                Select select = (Select) sqlStatement;
                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                try {
                    Master.executeSelect(plainSelect, true,
                            plotAtMost, plotEvery, plotDir, name, null);
                    // Output final result
                    String resultRel = NamingConfig.FINAL_RESULT_NAME;
                    RelationPrinter.print(resultRel);
                } catch (SQLexception e) {
                    System.out.println(e.getMessage());
                } catch (Exception e) {
                    throw e;
                } finally {
                    // Clean up intermediate results
                    BufferManager.unloadTempData();
                    CatalogManager.removeTempTables();
                }
            } else {
                System.out.println("Error - explain command supports "
                        + "only simple select queries");
            }
        }
    }

    /**
     * Executes input command, returns false iff
     * the input was a termination command.
     *
     * @param input input command to process
     * @throws Exception
     * @return false iff input was termination command
     */
    static boolean processInput(String input) throws Exception {
        // Delete semicolons if any
        input = input.replace(";", "");
        // Determine input category
        if (input.equals("quit")) {
            // Terminate console
            return false;
        } else if (input.startsWith("bench")) {
            processBenchCmd(input);
        } else if (input.startsWith("exp")) {
            String benchmark = Configuration.getProperty("BENCH", "IMDB");
            String queries = Configuration.getProperty(benchmark, "../imdb/queries");
            String newInput = "bench " + queries + " ";
            String warmup = StartupConfig.WARMUP_RUN ? "warmup/" : "";
            String memory = StartupConfig.Memory ? "memory/" : "";
            String output = "./" + warmup + memory + benchmark.toLowerCase() + "/";
            String caseName = GeneralConfig.TEST_CASE > 0 ? "_" + GeneralConfig.TEST_CASE : "";
            if (GeneralConfig.isParallel) {
                int spec = ParallelConfig.PARALLEL_SPEC;
                if (spec == 0) {
                    output += "DPOP_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                } else if (spec == 1) {
                    output += "DPDsync_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                } else if (spec == 2) {
                    output += "PSS_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                } else if (spec == 3) {
                    output += "PSA_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                } else if (spec == 4) {
                    output += "Root_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                } else if (spec == 5) {
                    output += "Leaf_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                } else if (spec == 6) {
                    output += "Tree_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 7) {
                    output += "PSJ_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 8) {
                    output += "SPS_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 9) {
                    if (GeneralConfig.TESTCACHE) {
                        output += "SSPT_" + ParallelConfig.NR_EXECUTORS + caseName + ".txt";
                    }
                    else {
                        output += "SSPT_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                    }
                }
                else if (spec == 10) {
                    String prefix = ParallelConfig.HEURISTIC_POLICY == 0 ? "CHPS_" : "SHPS_";
                    output += prefix + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 11) {
                    output += "DPL_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 12) {
                    output += "DPM_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 13) {
                    output += "DPOP_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 14) {
                    output += "CAPS_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
                else if (spec == 15) {
                    output += "PJ_" + ParallelConfig.EXE_THREADS + caseName + ".txt";
                }
            } else {
                output += "Seq_1.txt";
            }
            newInput += output;
            processBenchCmd(newInput);
        } else if (input.equals("compress")) {
            Compressor.compress();
        } else if (input.startsWith("exec")) {
            processFile(input);
        } else if (input.startsWith("set")) {
            changeConfig(input);
        } else if (input.startsWith("explain")) {
            String[] inputFrags = input.split("\\s");
            processExplain(inputFrags);
        } else if (input.equals("help")) {
            System.out.println("'bench <query dir> <output file>' to benchmark queries in *.sql files");
            System.out.println("'compress' to compress database");
            System.out.println("'exec <SQL file>' to execute file");
            System.out.println("'explain <Plot Dir> <Plot Bound> "
                    + "<Plot Frequency> <Query>' to visualize query execution");
            System.out.println("'set <parameter> <value>' to change the value of parameter");
            System.out.println("'help' for help");
            System.out.println("'index all' to index each column");
            System.out.println("'list' to list database tables");
            System.out.println("'load <table> <CSV file> <separator> <NULL representation>' "
                    + "to load table data from .csv file");
            System.out.println("'quit' for quit");
            System.out.println("Write SQL queries in a single line");
        } else if (input.equals("index all")) {
            Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
        } else if (input.equals("list")) {
            // Show overview of the database
            System.out.println(CatalogManager.currentDB.toString());
        } else if (input.startsWith("load ")) {
            processLoadCmd(input);
        } else if (input.isEmpty()) {
            // Nothing to do ...
        } else {
            try {
                processSQL(input, null, null);
            } catch (SQLexception e) {
                System.out.println(e.getMessage());
            }
        }
        return true;
    }

    /**
     * Change the value of parameter to the given value.
     *
     * @param input        input command to process
     */
    private static void changeConfig(String input) {
        // Delete semicolons if any
        input = input.replace(";", "");
        String[] input_array = input.split(" ");
        if (input_array.length == 3) {
            Configuration.setProperty(input_array[1], input_array[2]);
            System.out.println("Set " + input_array[1] + " to " + input_array[2] + " successfully!");
            loadConfigs();
        }
        else {
            System.out.println("Please input 'set <parameter> <value>' to change the value of parameter");
        }
    }

    public static int mapToParallelAlgorithm(String algoName) {
        int algoSpec = 0;
        switch (algoName) {
            case "DP": {
                algoSpec = 0;
                break;
            }
            case "SP-O": {
                algoSpec = 8;
                break;
            }
            case "SP-P": {
                algoSpec = 2;
                break;
            }
            case "SP-C": {
                algoSpec = 14;
                break;
            }
            case "SP-H": {
                algoSpec = 10;
                break;
            }
            case "Root": {
                algoSpec = 4;
                break;
            }
            case "Leaf": {
                algoSpec = 5;
                break;
            }
            case "Tree": {
                algoSpec = 6;
                break;
            }
            case "TP": {
                algoSpec = 9;
                break;
            }
        }
        return algoSpec;
    }

    /**
     * Set parameters based on the configuration file.
     */
    public static void loadConfigs() {
        // Number of test case
        GeneralConfig.TEST_CASE = Integer.parseInt(Configuration.getProperty("TEST_CASE", "1"));
        // Number of warmup run
        GeneralConfig.NR_WARMUP = Integer.parseInt(Configuration.getProperty("NR_WARMUP", "1"));
        // Number of executors in task parallel
        ParallelConfig.NR_EXECUTORS = Integer.parseInt(Configuration.getProperty("NR_EXECUTORS", "1"));
        // Batch size
        ParallelConfig.NR_BATCHES = Integer.parseInt(Configuration.getProperty("NR_BATCHES", "60"));
        // Whether to write results
        GeneralConfig.WRITE_RESULTS = Boolean.parseBoolean(
                Configuration.getProperty("WRITE_RESULTS", "true"));
        // Parallel algorithms
        ParallelConfig.PARALLEL_SPEC = mapToParallelAlgorithm(
                Configuration.getProperty("PARALLEL_ALGO", "DP"));
        // Whether to measure memory consumption
        StartupConfig.Memory = Boolean.parseBoolean(
                Configuration.getProperty("TEST_MEM", "false"));
    }

    /**
     * Run Skinner console, using database schema
     * at specified location.
     *
     * @param args path to database directory
     */
    public static void main(String[] args) throws Exception {
        // Verify number of command line arguments
        if (args.length < 1) {
            System.out.println("Error - specify the path"
                    + " to database directory!");
            return;
        }
        dbDir = args[0];
        // Initialize configuration file
        Configuration.initConfiguration(dbDir);

        // Load parameters from the command line
        if (args.length > 1) {
            ParallelConfig.EXE_THREADS = Integer.parseInt(args[1]);
        }
        else {
            int defaultNrThreads = Runtime.getRuntime().availableProcessors();
            ParallelConfig.EXE_THREADS = Integer.parseInt(Configuration.getProperty("THREADS",
                    String.valueOf(defaultNrThreads)));
        }
        loadConfigs();
		// Load database schema and initialize path mapping
        PathUtil.initSchemaPaths(dbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        // Load data and/or dictionary
        if (GeneralConfig.inMemory) {
            // In-memory data processing
            BufferManager.loadDB();
        } else {
            // Disc data processing (not fully implemented!) -
            // string dictionary is still loaded.
            BufferManager.loadDictionary();
        }
        // Create indexes
        Indexer.indexAll(StartupConfig.INDEX_CRITERIA);

        // Task parallel
        if (ParallelConfig.PARALLEL_SPEC == 9) {
            ThreadPool.initThreadsPool(2, ParallelConfig.PRE_THREADS);
        } else {
            ThreadPool.initThreadsPool(ParallelConfig.EXE_THREADS, ParallelConfig.PRE_THREADS);
        }

        System.out.println("SkinnerMT is using " +
                ParallelConfig.EXE_THREADS + " threads.");
        // Command line processing
        System.out.println("Enter 'help' for help and 'quit' to exit");
        Scanner scanner = new Scanner(System.in);
        boolean continueProcessing = true;
        while (continueProcessing) {
            System.out.print("> ");
            String input = scanner.nextLine();
            try {
                continueProcessing = processInput(input);
            } catch (Exception e) {
                System.err.println("Error processing command: ");
                e.printStackTrace();
            }
        }
        scanner.close();
        ThreadPool.close();
    }
}
