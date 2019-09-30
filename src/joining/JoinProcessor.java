package joining;

import java.util.*;

import config.GeneralConfig;
import config.LoggingConfig;
import config.NamingConfig;
import config.JoinConfig;
import joining.join.BatchQueryJoinMultiTracker;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import multiquery.GlobalContext;
import operators.Materialize;
import preprocessing.Context;
import query.ColumnRef;
import query.CommonQueryPrefix;
import query.QueryInfo;


/**
 * Controls the join phase.
 *
 * @author immanueltrummer
 */
public class JoinProcessor {
    /**
     * The number of join-related log entries
     * generated for the current query.
     */
    static int nrLogEntries = 0;

    /**
     * Executes the join phase and stores result in relation.
     * Also updates mapping from query column references to
     * database columns.
     *
     * @param queries      query to queries
     * @param preSummaries query execution contexts
     */
    public static void process(QueryInfo[] queries,
                               Context[] preSummaries) throws Exception {
        // Initialize statistics
//        JoinStats.nrTuples = 0;
//        JoinStats.nrIndexLookups = 0;
//        JoinStats.nrIndexEntries = 0;
//        JoinStats.nrUniqueIndexLookups = 0;
//        JoinStats.nrIterations = 0;
//        JoinStats.nrUctNodes = 0;
//        JoinStats.nrPlansTried = 0;
//        JoinStats.nrSamples = 0;
        // Initialize logging for new query
//		nrLogEntries = 0;
        // Initialize multi-way join operator

        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        int nrQueries = queries.length;
        //OldJoin[] oldJoins = new OldJoin[nrQueries];
        BatchQueryJoinMultiTracker batchQueryJoin = new BatchQueryJoinMultiTracker(queries, preSummaries);

        // Initialize UCT join order search tree
        UctNode[] roots = new UctNode[nrQueries];
        //int[] roundCtr = new int[nrQueries];
        int roundCtr = 0;
        for (int i = 0; i < nrQueries; i++) {
            //oldJoins[i] = new OldJoin(queries[i], preSummaries[i], JoinConfig.BUDGET_PER_EPISODE);
            roots[i] = new UctNode(0, queries[i], true);
            //roundCtr[i] = 0;
        }

        //while (!GlobalContext.checkStatus()) {
        while (GlobalContext.firstUnfinishedNum >= 0) {
            //we don't enable preprocessing, preprocessing is also on the UCT search

            int[][] orderList = new int[nrQueries][];
            for (int triedQuery = 0; triedQuery < nrQueries; triedQuery++) {
                if(GlobalContext.queryStatus[triedQuery])
                    continue;
                QueryInfo query = queries[triedQuery];
                int[] joinOrder = new int[query.nrJoined];
                roots[triedQuery].sample(roundCtr, joinOrder, policy);
                roundCtr++;
                orderList[triedQuery] = joinOrder;
            }

            HashMap<Integer, List<Integer>>[] batchGroups = new HashMap[nrQueries];
            HashSet<HashSet<Integer>> batchGroupSet = new HashSet<>();
            HashMap<Integer, HashSet<Integer>> batchGroupMap = new HashMap<>();
            int startQuery = GlobalContext.firstUnfinishedNum;
            for (int i = 0; i < nrQueries; i++) {
                int triedQuery = (startQuery + i) % nrQueries;
                if(GlobalContext.queryStatus[triedQuery])
                    continue;
                QueryInfo query = queries[triedQuery];
                CommonQueryPrefix commonQueryPrefix = query.decideReusable(i, orderList, startQuery);
                if(commonQueryPrefix != null) {
                    int reusedQuery = commonQueryPrefix.reusedQuery;
                    if (batchGroups[reusedQuery] == null)
                        batchGroups[reusedQuery] = new HashMap<>();
                    batchGroups[reusedQuery].putIfAbsent(commonQueryPrefix.prefixLen, new ArrayList<>());
                    batchGroups[reusedQuery].get(commonQueryPrefix.prefixLen).add(triedQuery);
                    HashSet<Integer> currentSet = batchGroupMap.get(reusedQuery);
                    currentSet.add(triedQuery);
                    batchGroupMap.put(triedQuery, currentSet);
                } else {
                    HashSet<Integer> currentSet = new HashSet<>();
                    currentSet.add(triedQuery);
                    batchGroupSet.add(currentSet);
                    batchGroupMap.put(triedQuery, currentSet);
                }
            }

//            batchGroupSet.forEach(i -> System.out.println("=====" + i.toString() + "====="));

            double[] reward = batchQueryJoin.execute(orderList, batchGroups, startQuery, batchGroupSet);
            for (int i = 0; i < nrQueries; i++) {
                if(GlobalContext.queryStatus[i])
                    continue;
                roots[i].updateReward(reward[i], orderList[i], 0);
            }


            int total = 0;
            for (int i = 0; i < nrQueries; i++) {
                if (!GlobalContext.queryStatus[i])
                    total++;
            }
            System.out.println("first:" + GlobalContext.firstUnfinishedNum);
            System.out.println("total unfinish:" + total);

            GlobalContext.aheadFirstUnfinish();

        }

//        for (int i = 0; i < nrQueries; i++) {
//            System.out.println("status:" + GlobalContext.queryStatus[i]);
//        }

        // Materialize result table
        for(int i = 0; i < nrQueries; i++) {
            Collection<ResultTuple> tuples = batchQueryJoin.result[i].getTuples();
            int nrTuples = tuples.size();
            System.out.println(i + ", size:" + nrTuples);
            if(i == GeneralConfig.testQuery) {
                List<ResultTuple> tupleslist = new ArrayList<>(tuples);
                tupleslist.sort(new Comparator<ResultTuple>() {
                    @Override
                    public int compare(ResultTuple o1, ResultTuple o2) {
                        for (int i = 0; i < o1.baseIndices.length; i++) {
                            if (o1.baseIndices[i] > o2.baseIndices[i])
                                return 1;
                            else if (o1.baseIndices[i] < o2.baseIndices[i])
                                return -1;
                        }
                        return 0;
                    }
                });
                for (ResultTuple tuple : tupleslist) {
                    System.out.println(Arrays.toString(tuple.baseIndices));
                }
            }

            log("Materializing join result with " + nrTuples + " tuples ...");
            String targetRelName = NamingConfig.JOINED_NAME + i;
            Materialize.execute(tuples, queries[i].aliasToIndex,
                    queries[i].colsForPostProcessing,
                    preSummaries[i].columnMapping, targetRelName);
            // Update processing context
            preSummaries[i].columnMapping.clear();
            for (ColumnRef postCol : queries[i].colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                preSummaries[i].columnMapping.put(postCol, newRef);
            }

//            PostProcessor.process(queries[i], preSummaries[i]);
//            String resultRel = NamingConfig.FINAL_RESULT_NAME;
//            RelationPrinter.print(resultRel);
        }
    }

    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry log entry to print
     */
    static void log(String logEntry) {
        if (nrLogEntries < LoggingConfig.MAX_JOIN_LOGS) {
            ++nrLogEntries;
            System.out.println(logEntry);
        }
    }
}
