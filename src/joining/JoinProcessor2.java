package joining;

import java.util.Arrays;
import java.util.Collection;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import config.JoinConfig;
import joining.join.OldJoin;
import joining.result.ResultTuple;
import joining.uct.BrueNode;
import operators.Materialize;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

/**
 * Controls the join phase.
 *
 * @author immanueltrummer
 */
public class JoinProcessor2 {
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
     * @param query   query to process
     * @param context query execution context
     */
    public static void process(QueryInfo query,
                               Context context) throws Exception {
        // Initialize statistics
        long startMillis = System.currentTimeMillis();
        JoinStats.nrTuples = 0;
        JoinStats.nrFastBacktracks = 0;
        JoinStats.nrIndexLookups = 0;
        JoinStats.nrIndexEntries = 0;
        JoinStats.nrUniqueIndexLookups = 0;
        JoinStats.nrIterations = 0;
        JoinStats.nrUctNodes = 0;
        JoinStats.nrPlansTried = 0;
        JoinStats.nrSamples = 0;
        // Initialize logging for new query
        nrLogEntries = 0;
        // Can we skip the join phase?
        if (query.nrJoined == 1 && PreConfig.PRE_FILTER) {
            String alias = query.aliases[0];
            String table = context.aliasToFiltered.get(alias);
            context.joinedTable = table;
            return;
        }
        // Initialize multi-way join operator
        OldJoin joinOp = new OldJoin(query, context,
                JoinConfig.LEARN_BUDGET_EPISODE);
        // Initialize UCT join order search tree
        BrueNode root = new BrueNode(0, query, true, joinOp);
//        BrueNode2 root = new BrueNode2(0, query, true, joinOp);
        // Initialize counters and variables
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;
        double maxReward = Double.NEGATIVE_INFINITY;
        int nrSampleTries = JoinConfig.SAMPLE_PER_LEARN;
        int nrJoined = query.nrJoined;
		int nExecutionTries = 0;
//		int executionBudget =  JoinConfig.START_EXECUTION_BUDGET_EPISODE;
        while (!joinOp.isFinished()) {
            // Learning phase
            joinOp.budget = JoinConfig.LEARN_BUDGET_EPISODE;
            for (int num = 0; num < nrSampleTries; num++) {
                ++roundCtr;
                int selectSwitch = nrJoined - ((int) ((roundCtr - 1) % nrJoined));
                double reward = root.sample(roundCtr, joinOrder, 0, selectSwitch);
//                MutableBoolean restart = new MutableBoolean(false);
//                double reward = root.sample(roundCtr, joinOrder, 0, selectSwitch, true, restart);
//                if(restart.booleanValue()) {
//                    roundCtr = 0;
//                }
                // Generate logging entries if activated
                log("Selected join order " + Arrays.toString(joinOrder));
                log("Obtained reward:\t" + reward);
                log("Table offsets:\t" + Arrays.toString(joinOp.tracker.tableOffset));
                log("Table cardinalities:\t" + Arrays.toString(joinOp.cardinalities));
            }

			//Execution phase
			int[] currentOptimalJoinOrder = new int[query.nrJoined];
			boolean finish = root.getOptimalPolicy(currentOptimalJoinOrder, 0);
			// System.out.println("Opt:" + Arrays.toString(currentOptimalJoinOrder));
//			System.out.println("Current Optimal:" + Arrays.toString(currentOptimalJoinOrder) + ", EntireOrder:" + finish);

			if(finish) {
//			    System.out.println("Current Optimal:" + Arrays.toString(currentOptimalJoinOrder));
//				joinOp.budget = executionBudget;
                joinOp.budget = JoinConfig.EXECUTION_BUDGET_EPISODE;
				root.executePhaseWithBudget(currentOptimalJoinOrder);
//                executionBudget *= JoinConfig.EXECUTION_BUDGET_INCREASE_DELTA;
			}
			nExecutionTries++;
        }

        //Clear node map
//        root.clearNodeMap();

        // Output most frequently used join order
        System.out.print("MFJO: ");
        for (int joinCtr = 0; joinCtr < query.nrJoined; ++joinCtr) {
            int table = joinOrder[joinCtr];
            String alias = query.aliases[table];
            System.out.print(alias + " ");
        }
        System.out.println();
        // Draw final plot if activated
//        if (query.explain) {
//            String plotName = "ucttreefinal.pdf";
//            String plotPath = Paths.get(query.plotDir, plotName).toString();
//            TreePlotter.plotTree(root, plotPath);
//        }
        // Update statistics
        JoinStats.nrSamples = roundCtr;
        JoinStats.avgReward = accReward / roundCtr;
        JoinStats.maxReward = maxReward;
        JoinStats.totalWork = 0;
        for (int tableCtr = 0; tableCtr < query.nrJoined; ++tableCtr) {
            if (tableCtr == joinOrder[0]) {
                JoinStats.totalWork += 1;
            } else {
                JoinStats.totalWork += Math.max(
                        joinOp.tracker.tableOffset[tableCtr], 0) /
                        (double) joinOp.cardinalities[tableCtr];
            }
        }
        // Measure pure join processing time (without materialization)
        JoinStats.pureJoinMillis = System.currentTimeMillis() - startMillis;
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
        // Materialize result table
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        int nrTuples = tuples.size();
        log("Materializing join result with " + nrTuples + " tuples ...");
        String targetRelName = NamingConfig.DEFAULT_JOINED_NAME;
        Materialize.execute(tuples, query.aliasToIndex,
                query.colsForPostProcessing,
                context.columnMapping, targetRelName);
        // Update processing context
        context.joinedTable = NamingConfig.DEFAULT_JOINED_NAME;
        context.columnMapping.clear();
        for (ColumnRef postCol : query.colsForPostProcessing) {
            String newColName = postCol.aliasName + "." + postCol.columnName;
            ColumnRef newRef = new ColumnRef(targetRelName, newColName);
            context.columnMapping.put(postCol, newRef);
        }
        // Store number of join result tuples
        JoinStats.skinnerJoinCard = CatalogManager.
                getCardinality(NamingConfig.DEFAULT_JOINED_NAME);
        // Measure execution time for join phase
        JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
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
