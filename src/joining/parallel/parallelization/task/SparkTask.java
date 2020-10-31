package joining.parallel.parallelization.task;

import expressions.ExpressionInfo;
import joining.parallel.join.JoinDoublePartitionWrapper;
import joining.parallel.join.JoinIntPartitionWrapper;
import joining.plan.JoinOrder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import query.ColumnRef;
import query.QueryInfo;

import java.util.*;

import org.apache.spark.sql.Column;
import query.SQLexception;

import javax.xml.crypto.Data;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SparkTask implements Callable<TaskResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
    /**
     * Shared atomic flags among all threads.
     * It indicates whether the join finishes.
     */
    private final AtomicInteger startIndex;
    /**
     * Next optimal join order broadcast by
     * the master thread.
     */
    public final AtomicReference<int[]> nextJoinOrder;
    /**
     * The table to generate partitions.
     */
    public final int splitTable;
    /**
     * Data frames of different tables.
     */
    private final Dataset<Row>[] tables;
    /**
     * Data frames of the partitioned table.
     */
    private final Dataset<Row>[] partitions;
    /**
     * Cache for storing specific join expression
     */
    private final Map<JoinOrder, List<Column>> joinExprsCache;
    /**
     * The executor id.
     */
    private final int executor;

    public SparkTask(QueryInfo query, AtomicInteger startIndex, int splitTable,
                     Dataset<Row>[] tables, Dataset<Row>[] partitions, int executor) {
        this.query = query;
        this.startIndex = startIndex;
        this.nextJoinOrder = new AtomicReference<>(new int[query.nrJoined]);
        this.tables = tables;
        this.partitions = partitions;
        this.joinExprsCache = new HashMap<>();
        this.splitTable = splitTable;
        this.executor = executor;
    }

    @Override
    public TaskResult call() throws Exception {
        int nrPartitions = partitions.length;
        int partitionIdx = startIndex.getAndIncrement();
        while (partitionIdx < nrPartitions) {
            // Step 1. get one join order via uct search
            JoinOrder joinOrder = new JoinOrder(nextJoinOrder.get());
            int[] order = joinOrder.order;
            // No broadcasting
            if (order[0] == order[1]) {
                continue;
            }
            // Have optimal join order
            System.out.println("Executor " + executor + ": Join " + Arrays.toString(order) + " for Batch " + partitionIdx);

            // Step 2. convert join order to join exprs.
            tables[splitTable] = partitions[partitionIdx];
            List<Column> joinExprs = joinExpressions(joinOrder, tables);
            int firstTable = order[0];
            Dataset<Row> targetDF = tables[firstTable];
            for (int joinCtr = 1; joinCtr < order.length; joinCtr++) {
                int nextTable = order[joinCtr];
                targetDF = targetDF.join(tables[nextTable], joinExprs.get(joinCtr), "inner");
            }
            targetDF.show(1);


            partitionIdx = startIndex.getAndIncrement();
        }
        return null;
    }

    private List<Column> joinExpressions(JoinOrder joinOrder, Dataset<Row>[] partitions) {
        if (joinExprsCache.containsKey(joinOrder)) {
            return joinExprsCache.get(joinOrder);
        }
        else {
            List<Column> columns = new ArrayList<>();
            int[] order = joinOrder.order;
            Set<Integer> availableTables = new HashSet<>();
            List<ExpressionInfo> remainingEquiPreds = new ArrayList<>(query.equiJoinPreds);
            for (int joinCtr = 0; joinCtr < order.length; joinCtr++) {
                int table = order[joinCtr];
                availableTables.add(table);
                Column column = null;
                // Iterate over remaining equi-join predicates
                Iterator<ExpressionInfo> equiPredsIter = remainingEquiPreds.iterator();
                while (equiPredsIter.hasNext()) {
                    ExpressionInfo equiPred = equiPredsIter.next();
                    if (availableTables.containsAll(
                            equiPred.aliasIdxMentioned)) {
                        Iterator<ColumnRef> iterator = equiPred.columnsMentioned.iterator();
                        ColumnRef leftRef = iterator.next();
                        ColumnRef rightRef = iterator.next();
                        int leftIndex = query.aliasToIndex.get(leftRef.aliasName);
                        int rightIndex = query.aliasToIndex.get(rightRef.aliasName);
                        if (column == null) {
                            column = partitions[leftIndex].col(leftRef.columnName).equalTo(
                                    partitions[rightIndex].col(rightRef.columnName)
                            );
                        }
                        else {
                            column = column.and(partitions[leftIndex].col(leftRef.columnName).equalTo(
                                    partitions[rightIndex].col(rightRef.columnName))
                            );
                        }
                        equiPredsIter.remove();
                    }
                }
                columns.add(column);
            }
            joinExprsCache.put(joinOrder, columns);
            return columns;
        }
    }
}
