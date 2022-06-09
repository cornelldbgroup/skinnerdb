package joining;

import catalog.CatalogManager;
import config.*;
import joining.parallel.indexing.OffsetIndex;
import joining.parallel.join.OldJoin;
import joining.parallel.parallelization.Parallelization;
import joining.parallel.parallelization.dpdsync.DPDSync;
import joining.parallel.parallelization.hybrid.AdaptiveHybridParallelization;
import joining.parallel.parallelization.hybrid.HybridParallelization;
import joining.parallel.parallelization.join.JoinParallelization;
import joining.parallel.parallelization.leaf.LeafParallelization;
import joining.parallel.parallelization.lockfree.DataParallelization;
import joining.parallel.parallelization.lockfree.LockFreeParallelization;
import joining.parallel.parallelization.root.RootParallelization;
import joining.parallel.parallelization.search.*;
import joining.parallel.parallelization.task.StandardParallelization;
import joining.parallel.parallelization.task.TaskParallelization;
import joining.parallel.parallelization.tree.TreeParallelization;
import joining.parallel.uct.NSPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import logs.LogUtils;
import net.sf.jsqlparser.expression.Expression;
import operators.Materialize;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.QueryStats;
import visualization.TreePlotter;

import java.nio.file.Paths;
import java.util.*;

/**
 * Controls the join phase.
 *
 * @author Anonymous
 *
 */
public class SeqJoinProcessor {
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
     * @param query		query to process
     * @param context	query execution context
     */
    public static void process(QueryInfo query,
                               Context context) throws Exception {
        // Initialize statistics
        long startMillis = System.currentTimeMillis();
        // there is no predicate to evaluate in join phase.
        if (query.equiJoinPreds.size() == 0 && query.nonEquiJoinPreds.size() == 0) {
            String targetRelName = NamingConfig.JOINED_NAME;
            Materialize.executeFromExistingTable(query.colsForPostProcessing,
                    context.columnMapping, targetRelName);
            // Measure execution time for join phase
            JoinStats.exeTime = 0;
            // Update processing context
            context.columnMapping.clear();
            for (ColumnRef postCol : query.colsForPostProcessing) {
                String newColName = postCol.aliasName + "." + postCol.columnName;
                ColumnRef newRef = new ColumnRef(targetRelName, newColName);
                context.columnMapping.put(postCol, newRef);
            }
            // Store number of join result tuples
            int skinnerJoinCard = CatalogManager.getCardinality(targetRelName);
            JoinStats.lastJoinCard = skinnerJoinCard;
            return;
        }
        JoinStats.nrTuples = 0;
        JoinStats.nrIndexLookups = 0;
        JoinStats.nrIndexEntries = 0;
        JoinStats.nrUniqueIndexLookups = 0;
        JoinStats.nrIterations = 0;
        JoinStats.nrUctNodes = 0;
        JoinStats.nrPlansTried = 0;
        JoinStats.nrSamples = 0;
        // Initialize logging for new query
        nrLogEntries = 0;
        // Initialize multi-way join operator
		/*
		DefaultJoin joinOp = new DefaultJoin(query, preSummary,
				LearningConfig.BUDGET_PER_EPISODE);
		*/
        Map<Expression, NonEquiNode> predToEval = new HashMap<>();
        int nrJoined = query.nrJoined;
        for (int i = 0; i < query.nonEquiJoinNodes.size(); i++) {
            // Compile predicate and store in lookup table
            Expression pred = query.nonEquiJoinPreds.get(i).finalExpression;
            NonEquiNode node = query.nonEquiJoinNodes.get(i);
            predToEval.put(pred, node);
        }
        OffsetIndex[][] threadOffsets = new OffsetIndex[1][nrJoined];
        for (int tableCtr = 0; tableCtr < nrJoined; tableCtr++) {
            threadOffsets[0][tableCtr] = new OffsetIndex();
        }
        OldJoin joinOp = new OldJoin(query, context, JoinConfig.BUDGET_PER_EPISODE,
                1, 0, predToEval, threadOffsets, null);
        System.out.println("Alias: " + Arrays.toString(query.aliases));
        System.out.println("Cards: " + Arrays.toString(joinOp.cardinalities));
        // Initialize UCT join order search tree
        NSPNode root = new NSPNode(0, query,
                JoinConfig.AVOID_CARTESIAN, 0, 0, 1);
        // Initialize counters and variables
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Initialize exploration weight
        switch (JoinConfig.EXPLORATION_POLICY) {
            case SCALE_DOWN:
                JoinConfig.EXPLORATION_WEIGHT = Math.sqrt(2);
                break;
            case STATIC:
            case REWARD_AVERAGE:
                // Nothing to do
                break;
            case ADAPT_TO_SAMPLE:
                final int nrSamples = 1000;
                double[] rewardSample = new double[nrSamples];
                for (int i=0; i<nrSamples; ++i) {
                    ++roundCtr;
                    rewardSample[i] =  root.sample(roundCtr, joinOrder, joinOp, SelectionPolicy.RANDOM);
                }
                Arrays.sort(rewardSample);
                double median = rewardSample[nrSamples/2];
                JoinConfig.EXPLORATION_WEIGHT = median;
                //System.out.println("Median:\t" + median);
                break;
        }
        // Get default action selection policy
        SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
        // Initialize counter until scale down
        long nextScaleDown = 1;
        // Initialize counter until memory loss
        long nextForget = 1;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;
        double maxReward = Double.NEGATIVE_INFINITY;
        while (!joinOp.isFinished()) {
            ++roundCtr;
            double reward = root.sample(roundCtr, joinOrder, joinOp, policy);
            if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
                joinOp.logs.add("Round: " + roundCtr + "\tJoin order: " + Arrays.toString(joinOrder));
                joinOp.logs.add("Reward: " + reward);
            }
            // Count reward except for final sample
            if (!joinOp.isFinished()) {
                accReward += reward;
                maxReward = Math.max(reward, maxReward);
            }
            switch (JoinConfig.EXPLORATION_POLICY) {
                case REWARD_AVERAGE:
                    double avgReward = accReward/roundCtr;
                    JoinConfig.EXPLORATION_WEIGHT = avgReward;
                    log("Avg. reward: " + avgReward);
                    break;
                case SCALE_DOWN:
                    if (roundCtr == nextScaleDown) {
                        JoinConfig.EXPLORATION_WEIGHT /= 10.0;
                        nextScaleDown *= 10;
                    }
                    break;
                case STATIC:
                case ADAPT_TO_SAMPLE:
                    // Nothing to do
                    break;
            }
            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr==nextForget) {
                root = new NSPNode(0, query,
                        JoinConfig.AVOID_CARTESIAN, 0, 0, 1);
                nextForget *= 10;
            }
            // Generate logging entries if activated
            log("Selected join order " + Arrays.toString(joinOrder));
            log("Obtained reward:\t" + reward);
            log("Table offsets:\t" + Arrays.toString(joinOp.tracker.tableOffset));
            log("Table cardinalities:\t" + Arrays.toString(joinOp.cardinalities));
        }
        // Write log to the local file.
        if (LoggingConfig.PARALLEL_JOIN_VERBOSE) {
            List<String>[] logs = new List[1];
            logs[0] = joinOp.logs;
            LogUtils.writeLogs(logs, "verbose/seq/" + QueryStats.queryName);
        }
        // Update statistics
        JoinStats.nrSamples = roundCtr;
        JoinStats.avgReward = accReward/roundCtr;
        JoinStats.maxReward = maxReward;
        JoinStats.totalWork = 0;
        for (int tableCtr=0; tableCtr<query.nrJoined; ++tableCtr) {
            if (tableCtr == joinOrder[0]) {
                JoinStats.totalWork += 1;
            } else {
                JoinStats.totalWork += Math.max(
                        joinOp.tracker.tableOffset[tableCtr],0)/
                        (double)joinOp.cardinalities[tableCtr];
            }
        }
        // Output final stats if join logging enabled
        if (LoggingConfig.MAX_JOIN_LOGS > 0) {
            System.out.println("Exploration weight:\t" +
                    JoinConfig.EXPLORATION_WEIGHT);
            System.out.println("Nr. rounds:\t" + roundCtr);
            System.out.println("Table offsets:\t" +
                    Arrays.toString(joinOp.tracker.tableOffset));
            System.out.println("Table cards.:\t" +
                    Arrays.toString(joinOp.cardinalities));
        }
        // Measure execution time for join phase
        JoinStats.exeTime = System.currentTimeMillis() - startMillis;
        // Materialize result table
        Collection<ResultTuple> tuples = new HashSet<>(joinOp.concurrentList);
        int nrTuples = tuples.size();
        log("Materializing join result with " + nrTuples + " tuples ...");
        String targetRelName = NamingConfig.JOINED_NAME;
        Materialize.execute(tuples, query.aliasToIndex,
                query.colsForPostProcessing,
                context.columnMapping, targetRelName);
        // Update processing context
        context.columnMapping.clear();
        for (ColumnRef postCol : query.colsForPostProcessing) {
            String newColName = postCol.aliasName + "." + postCol.columnName;
            ColumnRef newRef = new ColumnRef(targetRelName, newColName);
            context.columnMapping.put(postCol, newRef);
        }
        // Store number of join result tuples
        int skinnerJoinCard = CatalogManager.
                getCardinality(NamingConfig.JOINED_NAME);
        JoinStats.lastJoinCard = skinnerJoinCard;

        // Measure execution time for join phase
        JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
        JoinStats.matMillis = JoinStats.joinMillis - JoinStats.exeTime;
        System.out.println("Round count: " + roundCtr);
        System.out.println("Join Order: " + Arrays.toString(joinOrder));
        System.out.println("Join card: " + skinnerJoinCard + "\tJoin time:" + JoinStats.joinMillis);
        JoinStats.lastJoinCard = nrTuples;
    }
    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry	log entry to print
     */
    static void log(String logEntry) {
        if (nrLogEntries < LoggingConfig.MAX_JOIN_LOGS) {
            ++nrLogEntries;
            System.out.println(logEntry);
        }
    }

}
