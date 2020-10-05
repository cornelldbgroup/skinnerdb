package joining.parallel.parallelization.task;

import config.JoinConfig;
import config.ParallelConfig;
import config.StartupConfig;
import expressions.ExpressionInfo;
import joining.parallel.join.FixJoin;
import joining.parallel.join.SubJoin;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class DBTask implements Callable<TaskResult> {
    /**
     * The query to process.
     */
    private final QueryInfo query;
    /**
     * Multi-way join operator.
     */
    private final FixJoin spJoin;
    /**
     * Shared atomic flags among all threads.
     * It indicates whether the join finishes.
     */
    private final AtomicBoolean finish;
    /**
     * The best join order assigned to
     * each executor threads by the searching thread.
     */
    private int[][] bestJoinOrder;
    /**
     * The probability for each prefix in the best join order assigned to
     * each executor threads by the searching thread.
     */
    private double[][] bestProbs;
    /**
     * Multiple join operators for threads
     */
    private final List<FixJoin> fixJoins;

    public DBTask(QueryInfo query, FixJoin spJoin, AtomicBoolean finish,
                        int[][] bestJoinOrder, double[][] bestProbs,
                        List<FixJoin> fixJoins) {
        this.query = query;
        this.spJoin = spJoin;
        this.finish = finish;
        this.bestJoinOrder = bestJoinOrder;
        this.bestProbs = bestProbs;
        this.fixJoins = fixJoins;
    }

    @Override
    public TaskResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int tid = spJoin.tid;
        int nrTables = query.nrJoined;
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until memory loss
        long nextForget = 10;
        // Iterate until join result was generated
        double accReward = 0;
        // 0, 3, 1, 5, 2, 4
        if (tid != 0) {
            int[] joinOrder = new int[nrTables];
            int nrThreads = ParallelConfig.EXE_THREADS;
            int nextThread = 1;
            int lastCount = 0;
            int nextPeriod = ParallelConfig.C;
            double nextNum = 1;
            int nrExecutors = Math.min(ParallelConfig.NR_EXECUTORS, nrThreads - 1);
            double base = ParallelConfig.C;
            SPNode root = new SPNode(0, query, true, 1);
            SubJoin subJoin = new SubJoin(query, spJoin.preSummary, spJoin.budget, nrThreads, 0, spJoin.predToEval);
            subJoin.tracker = spJoin.tracker;
            while (!finish.get()) {
                ++roundCtr;
                double reward;
                reward = root.sample(roundCtr, joinOrder, subJoin, policy, true);
                // Count reward except for final sample
                if (!subJoin.isFinished()) {
                    accReward += reward;
                }
                else {
                    if (finish.compareAndSet(false, true)) {
                        System.out.println("Finish id: " + tid + "\t" + Arrays.toString(joinOrder) + "\t" + roundCtr);
                        subJoin.roundCtr = roundCtr;
                    }
                    break;
                }
                // assign the best join order to next thread.
                if (roundCtr == lastCount + nextPeriod && nrExecutors >= 1) {
                    int[] best = new int[nrTables];
                    double[] probs = new double[nrTables];
                    root.maxJoinOrder(best, 0, probs);
                    for (int i = 1; i < nrTables; i++) {
                        probs[i] = probs[i-1] * probs[i];
                    }
                    for (int i = 0; i < nrTables - 1; i++) {
                        probs[i] = probs[i] - probs[i+1];
                    }
                    boolean equal = true;
                    for (int i = 0; i < nrTables; i++) {
                        if (best[i] != bestJoinOrder[nextThread][i]) {
                            equal = false;
                            break;
                        }
                    }
                    if (!equal) {
//                        best = new int[]{0, 2, 3, 4, 1, 5, 6, 7};
                        System.arraycopy(best, 0, bestJoinOrder[nextThread], 0, nrTables);
                        System.arraycopy(probs, 0, bestProbs[nextThread], 0, nrTables);
                        bestJoinOrder[nextThread][nrTables] = 2;
                        fixJoins.get(nextThread).terminate.set(true);
                        System.out.println("Assign " + Arrays.toString(best)
                                + " to Thread " + nextThread + " at round " + roundCtr + " " + System.currentTimeMillis());
                    }

                    nextThread = (nextThread + 1) % (nrExecutors + 1);
                    if (nextThread == 0) {
                        nextThread = (nextThread + 1) % (nrExecutors + 1);
                    }
                    lastCount = (int) roundCtr;
                    nextNum = nextNum * base;
                    nextPeriod = (int) Math.round(nextNum);

                }

                // Consider memory loss
                if (JoinConfig.FORGET && roundCtr==nextForget) {
                    root = new SPNode(0, query, true, 1);
                    nextForget *= 10;
                }
                subJoin.roundCtr = roundCtr;
                spJoin.roundCtr = roundCtr;
                spJoin.statsInstance.nrTuples = subJoin.statsInstance.nrTuples;
            }
            // memory consumption
            if (StartupConfig.Memory) {
                JoinStats.treeSize = root.getSize(true);
                JoinStats.stateSize = subJoin.tracker.getSize();
            }
            // Materialize result table
            long timer2 = System.currentTimeMillis();
            System.out.println("Thread " + tid + " " + (timer2 - timer1)
                    + "\tRound: " + roundCtr + "\tOrder: " + Arrays.toString(joinOrder));
            Set<ResultTuple> tuples = subJoin.result.tuples;
//            spJoin.threadResultsList = subJoin.threadResultsList;
            return new TaskResult(tuples, subJoin.logs, tid);
        }
        else {
            // join order 0, 3, 1, 5, 2, 4
            int[] joinOrder = new int[]{1, 4, 5, 2, 3, 4};
            // initialize connection variables
            Connection con = null;
            Statement st = null;
            ResultSet rs = null;
            String con_url = "jdbc:monetdb://localhost:50000/jcch";
            // make a connection to the MonetDB server using JDBC URL starting with: jdbc:monetdb://
//            con = DriverManager.getConnection(con_url, "monetdb", "monetdb");
//            // make a statement object
//            st = con.createStatement();

            // assign predicates
            List<List<ExpressionInfo>> equiPreds = new ArrayList<>();
            List<List<ExpressionInfo>> unaryPreds = new ArrayList<>();
            List<List<ExpressionInfo>> nonEquiPreds = new ArrayList<>();
            List<Set<ColumnRef>> selectedColumns = new ArrayList<>();
            for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
                equiPreds.add(new ArrayList<>());
                unaryPreds.add(new ArrayList<>());
                nonEquiPreds.add(new ArrayList<>());
                selectedColumns.add(new HashSet<>());
            }
            List<ExpressionInfo> remainingEquiPreds = new ArrayList<>(query.equiJoinPreds);
            List<ExpressionInfo> remainingNonEquiPreds = new ArrayList<>();
            List<ExpressionInfo> remainingUnaryPreds = new ArrayList<>(query.unaryPredicates);

            // Initialize nonEquiJoinPredicates
            for (ExpressionInfo predInfo : query.nonEquiJoinPreds) {
                if (predInfo.aliasesMentioned.size() > 1) {
                    remainingNonEquiPreds.add(predInfo);
                }
            }
            // Iterate over join order positions, adding tables
            Set<Integer> availableTables = new HashSet<>();
            for (int joinCtr = 0; joinCtr < nrTables; joinCtr++) {
                int nextTable = joinOrder[joinCtr];
                String alias = query.aliases[nextTable];
                availableTables.add(nextTable);
                // Iterate over remaining equi-join predicates
                Iterator<ExpressionInfo> equiPredsIter = remainingEquiPreds.iterator();
                while (equiPredsIter.hasNext()) {
                    ExpressionInfo equiPred = equiPredsIter.next();
                    if (availableTables.containsAll(
                            equiPred.aliasIdxMentioned)) {
                        equiPreds.get(joinCtr).add(equiPred);
                        Iterator<Integer> tableIter = equiPred.aliasIdxMentioned.iterator();
                        int table1 = tableIter.next();
                        int table2 = tableIter.next();
                        int pos1 = tablePos(joinOrder, table1);
                        int pos2 = tablePos(joinOrder, table2);
                        Iterator<ColumnRef> colIter = equiPred.columnsMentioned.iterator();
                        ColumnRef col1 = colIter.next();
                        ColumnRef col2 = colIter.next();
                        ColumnRef priorCol = col1.aliasName.equals(alias) ? col2 : col1;
                        for (int prevCtr = Math.min(pos1, pos2); prevCtr < joinCtr; prevCtr++) {
                            selectedColumns.get(prevCtr).add(priorCol);
                        }
                        equiPredsIter.remove();
                    }
                }
                // Iterate over remaining other predicates
                Iterator<ExpressionInfo> nonEquiPredsIter =
                        remainingNonEquiPreds.iterator();
                int otherCtr = Math.max(1, joinCtr);
                while (nonEquiPredsIter.hasNext()) {
                    ExpressionInfo pred = nonEquiPredsIter.next();
                    if (availableTables.containsAll(
                            pred.aliasIdxMentioned)) {
                        nonEquiPreds.get(otherCtr).add(pred);
                        Iterator<Integer> tableIter = pred.aliasIdxMentioned.iterator();
                        int priorPos = nrTables;
                        while (tableIter.hasNext()) {
                            int pos = tablePos(joinOrder, tableIter.next());
                            priorPos = Math.min(pos, priorPos);
                        }
                        Iterator<ColumnRef> colIter = pred.columnsMentioned.iterator();
                        Set<ColumnRef> selectedRefs = new HashSet<>();
                        while (colIter.hasNext()) {
                            ColumnRef mentionedRef = colIter.next();
                            if (!mentionedRef.aliasName.equals(alias)) {
                                selectedRefs.add(mentionedRef);
                            }
                        }
                        for (int prevCtr = priorPos; prevCtr < joinCtr; prevCtr++) {
                            selectedColumns.get(prevCtr).addAll(selectedRefs);
                        }
                        nonEquiPredsIter.remove();
                    }
                }
                Iterator<ExpressionInfo> unaryPredsIter =
                        remainingUnaryPreds.iterator();
                while (unaryPredsIter.hasNext()) {
                    ExpressionInfo pred = unaryPredsIter.next();
                    if (availableTables.containsAll(
                            pred.aliasIdxMentioned)) {
                        unaryPreds.get(otherCtr).add(pred);
                        unaryPredsIter.remove();
                    }
                }
            }
            // Drop all views
//            for (int joinCtr = nrTables - 2; joinCtr >= 1; joinCtr--) {
//                String query = "drop view if exists T_" + joinCtr + ";";
//                st.executeUpdate(query);
//            }
            // Execute SQL query which returns a ResultSet object
            for (int joinCtr = 1; joinCtr < nrTables; joinCtr++) {
                String query = buildSQL(joinOrder, joinCtr, spJoin.query,
                        equiPreds.get(joinCtr), nonEquiPreds.get(joinCtr),
                        unaryPreds.get(joinCtr), selectedColumns.get(joinCtr));
                System.out.println(query);
//                st.execute(query);
            }

//            // Get meta data and print column names with their type
//            ResultSetMetaData md = rs.getMetaData();
//            for (int i = 1; i <= md.getColumnCount(); i++) {
//                System.out.print(md.getColumnName(i) + ":" + md.getColumnTypeName(i) + "\t");
//            }

            // Drop all views
            for (int joinCtr = 1; joinCtr < nrTables - 1; joinCtr++) {
                String query = "drop view T_" + joinCtr + ";";
//                st.executeUpdate(query);
            }

            // Close (server) resource as soon as we are done processing data
            if (rs != null) {
                rs.close();
            }
            return null;
        }
    }

    public String buildSQL(int[] joinOrder, int joinIndex, QueryInfo query,
                           List<ExpressionInfo> equiPreds, List<ExpressionInfo> nonEquiPreds,
                           List<ExpressionInfo> unaryPreds, Set<ColumnRef> selectedCols) {
        StringBuilder buffer = new StringBuilder();
        String viewName = "T_" + joinIndex;
        String preView = "T_" + (joinIndex - 1);
        // Create a view
        if (joinIndex < query.nrJoined - 1) {
            buffer.append("create view ").append(viewName);
            List<String> viewColumns = new ArrayList<>();
            for (ColumnRef columnRef: selectedCols) {
                String viewColName = columnRef.aliasName + "_" + columnRef.columnName;
                viewColumns.add(viewColName);
            }
            buffer.append(" (").append(String.join(",", viewColumns)).append(") as\n");
        }
        buffer.append("select\n");
        Map<String, String> aliasMapping = new HashMap<>(query.nrJoined);
        for (int joinCtr = 0; joinCtr < joinIndex; joinCtr++) {
            int nextTable = joinOrder[joinCtr];
            String tableAlias = query.aliases[nextTable];
            String tableName = joinIndex == 1 ?
                    query.aliasToTable.get(tableAlias) : preView;
            aliasMapping.put(tableAlias, tableName);
        }
        int nextTable = joinOrder[joinIndex];
        String tableAlias = query.aliases[nextTable];
        String tableName = query.aliasToTable.get(tableAlias);
        aliasMapping.put(tableAlias, tableName);
        // Selected columns
        List<String> columns = new ArrayList<>();
        for (ColumnRef columnRef: selectedCols) {
            String selectName = mapColumnName(columnRef, aliasMapping);
            columns.add(selectName);
        }
        if (selectedCols.isEmpty()) {
            columns.add("count(*)");
        }
        buffer.append(String.join(",\n", columns)).append("\n");
        // From tables
        buffer.append("from\n");

        String intermediateTable = joinIndex == 1 ?
                query.aliasToTable.get(query.aliases[0]) : preView;
        buffer.append(intermediateTable);
        if (!query.temporaryTables.contains(nextTable)) {
            buffer.append(",\n");
            String tableStr = tableName.equals(tableAlias) ? tableName : tableName + " " + tableAlias;
            buffer.append(tableStr);
        }
        buffer.append("\n");

        // Where clause
        buffer.append("where\n");
        // Add unary predicate
        List<String> predicates = new ArrayList<>();
        for (ExpressionInfo unaryPred: unaryPreds) {
            predicates.add(unaryPred.toString());
        }
        // Add equi-join predicates
        for (ExpressionInfo equiPred: equiPreds) {
            Iterator<ColumnRef> iterator = equiPred.columnsMentioned.iterator();
            ColumnRef left = iterator.next();
            String leftName = mapColumnName(left, aliasMapping);
            ColumnRef right = iterator.next();
            String rightName = mapColumnName(right, aliasMapping);
            predicates.add(leftName + " = " + rightName);
        }
        // Add non-equi predicates
        if (query.temporaryTables.contains(nextTable)) {
            for (ExpressionInfo nonEquiPred: nonEquiPreds) {
                Expression expression = nonEquiPred.originalExpression;
                if (expression instanceof NotExpression) {
                    StringBuilder subBuffer = new StringBuilder();
                    subBuffer.append("exists (\n");
                    subBuffer.append("select\n\t*\nfrom\n\t");
                    subBuffer.append(tableName).append(" ").append(tableAlias).append("\nwhere\n");
                    SubQueryTest subQueryTest = new SubQueryTest(query, aliasMapping);
                    expression.accept(subQueryTest);
                    String subPredicates = String.join("\nand ", subQueryTest.predicates);
                    subBuffer.append(subPredicates).append("\n)");
                    predicates.add(subBuffer.toString());
                }
                else if (expression instanceof AndExpression && ((AndExpression) expression).isNot()) {
                    StringBuilder subBuffer = new StringBuilder();
                    subBuffer.append("not exists (\n");
                    subBuffer.append("select\n\t*\nfrom\n\t");
                    subBuffer.append(tableName).append(" ").append(tableAlias).append("\nwhere\n");
                    SubQueryTest subQueryTest = new SubQueryTest(query, aliasMapping);
                    expression.accept(subQueryTest);
                    String subPredicates = String.join("\nand ", subQueryTest.predicates);
                    subBuffer.append(subPredicates).append("\n)");
                    predicates.add(subBuffer.toString());
                }
                else {
                    System.out.println("Not implemented");
                }
            }
        }
        buffer.append(String.join("\nand ", predicates));
        buffer.append(";\n");
        return buffer.toString();
    }

    public String mapColumnName(ColumnRef columnRef, Map<String, String> aliasMapping) {
        String alias = columnRef.aliasName;
        String column = columnRef.columnName;
        String mappedAlias = aliasMapping.get(alias);
        if (mappedAlias.charAt(0) == 'T' && mappedAlias.charAt(1) == '_') {
            return mappedAlias + "." + alias + "_" + column;
        }
        else {
            if (mappedAlias.equals(alias))
                return mappedAlias + "." + column;
            else
                return alias + "." + column;
        }
    }

    int tablePos(int[] order, int table) {
        int nrTables = order.length;
        for (int pos = 0; pos < nrTables; ++pos) {
            if (order[pos] == table) {
                return pos;
            }
        }
        return -1;
    }
}
