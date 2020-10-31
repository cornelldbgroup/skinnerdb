package joining.parallel.parallelization.task;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import console.SkinnerCmd;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import joining.parallel.join.*;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.progress.ParallelProgressTracker;
import joining.parallel.threads.ThreadPool;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import scala.Array;
import statistics.JoinStats;
import statistics.QueryStats;
import types.TypeUtil;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * Initialize multiple executors where each executor
 * can submit spark tasks in parallel. The master thread
 * keeps learning the optimal join order based on sampling
 * data and broadcast the learning knowledge to each
 * executor in a round-robin way.
 *
 * @author Ziyun Wei
 */
public class SparkParallelization extends Parallelization {
    private final SubJoin masterJoin;
    /**
     * Multiple join operators for threads
     */
    private final List<SparkJoin> spJoins = new ArrayList<>();
    /**
     * initialization of parallelization
     *
     * @param nrThreads the number of threads
     * @param budget
     * @param query     select query with join predicates
     * @param context   query execution context
     */
    public SparkParallelization(int nrThreads, int budget, QueryInfo query, Context context) throws Exception {
        super(nrThreads, budget, query, context);
        // Initialize multi-way join operator
        int nrExecutors = Math.min(ParallelConfig.NR_EXECUTORS, nrThreads - 1);
        for (int eid = 0; eid < nrExecutors; eid++) {
            SparkJoin sparkJoin = new SparkJoin(query, context, eid);
            spJoins.add(sparkJoin);
        }
        // Compile predicates
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        ParallelProgressTracker tracker = new ParallelProgressTracker(query.nrJoined, 1, 1);
        masterJoin = new SubJoin(query, context, budget, 1, 0, predToEval);
        masterJoin.tracker = tracker;
    }
    @Override
    public void execute(Set<ResultTuple> resultList) throws Exception {
        int nrJoined = query.nrJoined;
        // Initialize a thread pool.
        ExecutorService executorService = ThreadPool.executorService;
        // Mutex shared by multiple threads.
        AtomicInteger startIndex = new AtomicInteger(0);
        List<Callable<TaskResult>> tasks = new ArrayList<>();
        // Initialize spark instance
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL")
                .config("spark.driver-memory", "10g")
                .config("spark.master", "local")
                .config("spark.scheduler.mode", "FAIR")
                .getOrCreate();

        // Get cardinality of tables
        Dataset<Row>[] datasets = new Dataset[nrJoined];
        long maxCardinality = -1;
        int largeTable = -1;
        for (Map.Entry<String, Integer> entry: query.aliasToIndex.entrySet()) {
            StructType schema = new StructType();
            String alias = entry.getKey();
            int index = entry.getValue();
            TableInfo tableInfo = CatalogManager.currentDB.nameToTable.get(alias);
            for (String columnName: tableInfo.columnNames) {
                ColumnInfo columnInfo = tableInfo.nameToCol.get(columnName);
                DataType columnType = TypeUtil.toScalaType(columnInfo.type);
                schema = schema.add(columnInfo.name, columnType, false);
            }
            Dataset<Row> dataset = spark.read().option("delimiter", "|")
                    .schema(schema)
                    .csv(SkinnerCmd.dbDir + "/csv/" + alias + ".tbl");
            long cardinality = dataset.count();
            if (cardinality > maxCardinality) {
                maxCardinality = cardinality;
                largeTable = index;
            }
            datasets[index] = dataset;
        }
        double[] partitionRatioArray =  new double[100];
        Arrays.fill(partitionRatioArray, 0.01);
        Dataset<Row> targetDF = datasets[largeTable];
        Dataset<Row>[] randomPartitions = targetDF.randomSplit(partitionRatioArray, 11L);

        int nrExecutors = spJoins.size();
        // Initialize task for each executor
        List<SparkTask> sparkTasks = new ArrayList<>(nrExecutors);
        for (int executor = 0; executor < nrExecutors; executor++) {
            SparkTask sparkTask = new SparkTask(
                    query, startIndex,
                    largeTable, datasets,
                    randomPartitions, executor
            );
            tasks.add(sparkTask);
            sparkTasks.add(sparkTask);
        }
        // Initialize the master thread
        tasks.add(() -> {
            int[] joinOrder = new int[nrJoined];
            int nextExecutor = 0;
            int roundCtr = 0;
            int lastCount = 0;
            int nextPeriod = ParallelConfig.C;
            SPNode root = new SPNode(0, query, true, 1);
            SelectionPolicy policy = SelectionPolicy.UCB1;
            while (roundCtr < 200) {
                ++roundCtr;
                double reward = root.sample(roundCtr, joinOrder, masterJoin, policy, true);
                // Count reward except for final sample
                if (masterJoin.isFinished()) {
                    break;
                }
                // Assign the best join order to next executor.
                if (roundCtr == lastCount + nextPeriod) {
                    System.out.println("Assign " + Arrays.toString(joinOrder) + " to Executor " + nextExecutor);
                    sparkTasks.get(nextExecutor).nextJoinOrder.updateAndGet(order -> joinOrder.clone());
                    nextExecutor = (nextExecutor + 1) % nrExecutors;
                    lastCount = roundCtr;
                }
            }
            return null;
        });

        long executionStart = System.currentTimeMillis();
        List<Future<TaskResult>> futures = executorService.invokeAll(tasks);
//        TaskResult result = executorService.invokeAny(tasks);
        long executionEnd = System.currentTimeMillis();
        JoinStats.exeTime = executionEnd - executionStart;
        futures.forEach(futureResult -> {
            try {
                TaskResult result = futureResult.get();
                if (result != null)
                    resultList.addAll(result.result);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

        });

        System.out.println("Result Set: " + resultList.size());
    }
}
