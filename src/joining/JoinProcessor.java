package joining;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;

import catalog.CatalogManager;
import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import config.JoinConfig;
import joining.join.OldJoin;
import joining.result.ResultTuple;
import joining.uct.ExplorationWeightPolicy;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;
import operators.Materialize;
import preprocessing.Context;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;
import visualization.TreePlotter;

/**
 * Controls the join phase.
 * 
 * @author immanueltrummer
 *
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
	 * @param query		query to process
	 * @param context	query execution context
	 */
	public static void process(QueryInfo query, 
			Context context) throws Exception {
        // Initialize statistics
		long startMillis = System.currentTimeMillis();
        JoinStats.initializeJoinStats();

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
				JoinConfig.BUDGET_PER_EPISODE);
		// Initialize UCT join order search tree
		UctNode root = new UctNode(0, query, 
				JoinConfig.AVOID_CARTESIANS, joinOp);
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
				rewardSample[i] = root.sample(
						roundCtr, joinOrder, 
						SelectionPolicy.RANDOM);				
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
			double reward = root.sample(roundCtr, joinOrder, policy);
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
				root = new UctNode(roundCtr, query, true, joinOp);
				nextForget *= 10;
			}
			// Generate logging entries if activated
			log("Selected join order " + Arrays.toString(joinOrder));
			log("Obtained reward:\t" + reward);
			log("Table offsets:\t" + Arrays.toString(joinOp.tracker.tableOffset));
			log("Table cardinalities:\t" + Arrays.toString(joinOp.cardinalities));
			// Generate plots if activated
			if (query.explain && plotCtr<query.plotAtMost && 
					roundCtr % query.plotEvery==0) {
				String plotName = "ucttree" + plotCtr + ".pdf";
				String plotPath = Paths.get(query.plotDir, plotName).toString();
				TreePlotter.plotTree(root, plotPath);
				++plotCtr;
			}
		}
		// Output most frequently used join order
		root.sample(roundCtr, joinOrder, SelectionPolicy.MAX_VISIT);
		System.out.print("MFJO: ");
		for (int joinCtr=0; joinCtr<query.nrJoined; ++joinCtr) {
			int table = joinOrder[joinCtr];
			String alias = query.aliases[table];
			System.out.print(alias + " ");
		}
		System.out.println();
		// Draw final plot if activated
		if (query.explain) {
			String plotName = "ucttreefinal.pdf";
			String plotPath = Paths.get(query.plotDir, plotName).toString();
			TreePlotter.plotTree(root, plotPath);
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
		materialize(query, context, joinOp.result.tuples);
		// Measure execution time for join phase
		JoinStats.joinMillis = System.currentTimeMillis() - startMillis;
	}

	/**
	 * Materialize join results into the temperary result table.
	 *
	 * @param query			query to process
	 * @param context		query execution context
	 * @param tuples		join results
	 */
	public static void materialize(QueryInfo query,
								   Context context,
								   Collection<ResultTuple> tuples) throws Exception {
		// Materialize result table
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
