package joining;

import java.nio.file.Paths;
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
public class JoinProcessorNew {
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
        BatchQueryJoinMultiTracker batchQueryJoinMultiTracker = new BatchQueryJoinMultiTracker(queries, preSummaries);

        // Initialize UCT join order search tree
        UctNode[] roots = new UctNode[nrQueries];
        //int[] roundCtr = new int[nrQueries];
        int roundCtr = 0;
		for (int i = 0; i < nrQueries; i++) {
            //oldJoins[i] = new OldJoin(queries[i], preSummaries[i], JoinConfig.BUDGET_PER_EPISODE);
            roots[i] = new UctNode(0, queries[i], true);
            //roundCtr[i] = 0;
        }

        while (GlobalContext.firstUnfinishedNum >= 0) {
            //we don't enable preprocessing, preprocessing is also on the UCT search
            int startQuery = GlobalContext.firstUnfinishedNum;
            int[][] orderList = new int[nrQueries][];
            HashMap<Integer, List<Integer>>[] batchGroups = new HashMap[nrQueries];
            HashSet<HashSet<Integer>> batchGroupSet = new HashSet<>();
            HashMap<Integer, HashSet<Integer>> batchGroupMap = new HashMap<>();
            for (int i = 0; i < nrQueries; i++) {
                int prefixLen = 0;
                int triedQuery = (startQuery + i) % nrQueries;
                QueryInfo query = queries[triedQuery];
                if(GlobalContext.queryStatus[triedQuery])
                	continue;
                int[] joinOrder = new int[query.nrJoined];
                CommonQueryPrefix commonQueryPrefix = query.findShortOrders(startQuery, orderList, i);
                if (commonQueryPrefix != null) {
                    int reusedQuery = commonQueryPrefix.reusedQuery;
                    if (batchGroups[reusedQuery] == null)
                        batchGroups[reusedQuery] = new HashMap<>();
                    prefixLen = commonQueryPrefix.prefixLen;
                    batchGroups[reusedQuery].putIfAbsent(prefixLen, new ArrayList<>());
                    batchGroups[reusedQuery].get(prefixLen).add(triedQuery);
                    System.arraycopy(commonQueryPrefix.joinOrder, 0, joinOrder, 0, commonQueryPrefix.prefixLen);
                    //System.out.println("reuse:" + reusedQuery +", base" + triedQuery + ", order:" + Arrays.toString(commonQueryPrefix.joinOrder) + ", reusedLen:" + commonQueryPrefix.prefixLen + ", total Len" + joinOrder.length);

                    //batchGroupMap.forEach((a, b) -> b.forEach(c -> System.out.println(a+"," +c)));
                    //System.out.println("Reuse Query:"+ reusedQuery);
                    HashSet<Integer> currentSet = batchGroupMap.get(reusedQuery);
                    currentSet.add(triedQuery);
                    batchGroupMap.put(triedQuery, currentSet);
                } else {
                    HashSet<Integer> currentSet = new HashSet<>();
                    currentSet.add(triedQuery);
                    batchGroupSet.add(currentSet);
                    batchGroupMap.put(triedQuery, currentSet);
                }
                if(prefixLen > 0)
					roots[triedQuery].ahead(roundCtr, joinOrder, 0, prefixLen, policy);
                	//roots[triedQuery].ahead(roundCtr[triedQuery], joinOrder, 0, prefixLen, policy);
                else {

                      roots[triedQuery].sample(roundCtr, joinOrder, policy);

//                    boolean canStop;
//                    do {
//                        canStop = true;
//                        roots[triedQuery].sample(roundCtr, joinOrder, policy);
//                        for (int j = 0; j < i; j++) {
//                            QueryInfo previousQuery = queries[(startQuery + j) % nrQueries];
//                            int size = previousQuery.findSamePrefixLen(triedQuery, joinOrder).size();
//                            System.out.println("size:" + size);
//                            canStop &= (size == 0);
//                        }
//                        System.out.println("join order:" + Arrays.toString(joinOrder) + ", can stop:" + canStop);
//                    } while (!canStop);

                    //roots[triedQuery].sample(roundCtr[triedQuery], joinOrder, policy);
                }
                //roundCtr[triedQuery]++;
                roundCtr++;
				orderList[triedQuery] = joinOrder;
                //System.out.println("query: " + triedQuery + ", join order: " + Arrays.toString(joinOrder));
            }
            //Run batch query process
//            for (int i = 0; i < nrQueries; i++) {
//                System.out.println("join order:" + Arrays.toString(orderList[i]) + ", status" + GlobalContext.queryStatus[i]);
//            }

//            System.out.println("***************************");
            double[] reward = batchQueryJoinMultiTracker.execute(orderList, batchGroups, startQuery, batchGroupSet);
            for (int i = 0; i < nrQueries; i++) {
                if(GlobalContext.queryStatus[i])
                    continue;
                //System.out.println("join order:" + Arrays.toString(orderList[i]));
                //System.out.println(reward[i]);
                roots[i].updateReward(reward[i], orderList[i], 0);
            }
            GlobalContext.aheadFirstUnfinish();
            //System.out.println("========================");
            int total = 0;
            for (int i = 0; i < nrQueries; i++) {
                if (!GlobalContext.queryStatus[i])
                    total++;
                System.out.println("status:" + GlobalContext.queryStatus[i]);
            }
            System.out.println("first:" + GlobalContext.firstUnfinishedNum);
            System.out.println("total unfinish:" + total);

//            if(GlobalContext.queryStatus[GeneralConfig.testQuery]) {
//                String targetRelName = "q13";
//                int qr = GeneralConfig.testQuery;
//                Materialize.execute(batchQueryJoin.result[qr].getTuples(), queries[qr].aliasToIndex,
//                        queries[qr].colsForPostProcessing,
//                        preSummaries[qr].columnMapping, targetRelName);
//                // Update processing context
//                preSummaries[qr].columnMapping.clear();
//                for (ColumnRef postCol : queries[qr].colsForPostProcessing) {
//                    String newColName = postCol.aliasName + "." + postCol.columnName;
//                    ColumnRef newRef = new ColumnRef(targetRelName, newColName);
//                    preSummaries[qr].columnMapping.put(postCol, newRef);
//                }
//                PostProcessor.process(queries[qr], preSummaries[qr]);
//                String resultRel = NamingConfig.FINAL_RESULT_NAME;
//                RelationPrinter.print(resultRel);
//            }
        }

//        for (int i = 0; i < nrQueries; i++) {
//            System.out.println("status:" + GlobalContext.queryStatus[i]);
//        }

        // Initialize counters and variables

//		long roundCtr = 0;
//		// Initialize exploration weight
//		switch (JoinConfig.EXPLORATION_POLICY) {
//			case SCALE_DOWN:
//				JoinConfig.EXPLORATION_WEIGHT = Math.sqrt(2);
//				break;
//			case STATIC:
//			case REWARD_AVERAGE:
//				// Nothing to do
//				break;
//			case ADAPT_TO_SAMPLE:
//				final int nrSamples = 1000;
//				double[] rewardSample = new double[nrSamples];
//				for (int i=0; i<nrSamples; ++i) {
//					++roundCtr;
//					rewardSample[i] = root.sample(
//							roundCtr, joinOrder,
//							SelectionPolicy.RANDOM);
//				}
//				Arrays.sort(rewardSample);
//				double median = rewardSample[nrSamples/2];
//				JoinConfig.EXPLORATION_WEIGHT = median;
//				//System.out.println("Median:\t" + median);
//				break;
//		}
//		// Get default action selection policy
//		SelectionPolicy policy = JoinConfig.DEFAULT_SELECTION;
//		// Initialize counter until scale down
//		long nextScaleDown = 1;
//		// Initialize counter until memory loss
//		long nextForget = 1;
//		// Initialize plot counter
//		int plotCtr = 0;
//		// Iterate until join result was generated
//		double accReward = 0;
//		double maxReward = Double.NEGATIVE_INFINITY;
//		//while (!joinOp.isFinished()) {
//			++roundCtr;
//			double reward = root.sample(roundCtr, joinOrder, policy);
//			// Count reward except for final sample
//			if (!joinOp.isFinished()) {
//				accReward += reward;
//				maxReward = Math.max(reward, maxReward);
//			}
//			switch (JoinConfig.EXPLORATION_POLICY) {
//			case REWARD_AVERAGE:
//				double avgReward = accReward/roundCtr;
//				JoinConfig.EXPLORATION_WEIGHT = avgReward;
//				log("Avg. reward: " + avgReward);
//				break;
//			case SCALE_DOWN:
//				if (roundCtr == nextScaleDown) {
//					JoinConfig.EXPLORATION_WEIGHT /= 10.0;
//					nextScaleDown *= 10;
//				}
//				break;
//			case STATIC:
//			case ADAPT_TO_SAMPLE:
//				// Nothing to do
//				break;
//			}
//			// Consider memory loss
//			if (JoinConfig.FORGET && roundCtr==nextForget) {
//				root = new UctNode(roundCtr, query, true, joinOp);
//				nextForget *= 10;
//			}
//			// Generate logging entries if activated
//			log("Selected join order " + Arrays.toString(joinOrder));
//			log("Obtained reward:\t" + reward);
//			log("Table offsets:\t" + Arrays.toString(joinOp.tracker.tableOffset));
//			log("Table cardinalities:\t" + Arrays.toString(joinOp.cardinalities));
//			// Generate plots if activated
//			if (query.explain && plotCtr<query.plotAtMost &&
//					roundCtr % query.plotEvery==0) {
//				String plotName = "ucttree" + plotCtr + ".pdf";
//				String plotPath = Paths.get(query.plotDir, plotName).toString();
//				TreePlotter.plotTree(root, plotPath);
//				++plotCtr;
//			}
//		//}
//		// Draw final plot if activated
//		if (query.explain) {
//			String plotName = "ucttreefinal.pdf";
//			String plotPath = Paths.get(query.plotDir, plotName).toString();
//			TreePlotter.plotTree(root, plotPath);
//		}
//		// Update statistics
//		JoinStats.nrSamples = roundCtr;
//		JoinStats.avgReward = accReward/roundCtr;
//		JoinStats.maxReward = maxReward;
//		JoinStats.totalWork = 0;
//		for (int tableCtr=0; tableCtr<query.nrJoined; ++tableCtr) {
//			if (tableCtr == joinOrder[0]) {
//				JoinStats.totalWork += 1;
//			} else {
//				JoinStats.totalWork += Math.max(
//						joinOp.tracker.tableOffset[tableCtr],0)/
//						(double)joinOp.cardinalities[tableCtr];
//			}
//		}
//		// Output final stats if join logging enabled
//		if (LoggingConfig.MAX_JOIN_LOGS > 0) {
//			System.out.println("Exploration weight:\t" +
//					JoinConfig.EXPLORATION_WEIGHT);
//			System.out.println("Nr. rounds:\t" + roundCtr);
//			System.out.println("Table offsets:\t" +
//					Arrays.toString(joinOp.tracker.tableOffset));
//			System.out.println("Table cards.:\t" +
//					Arrays.toString(joinOp.cardinalities));
//		}
		// Materialize result table
        for(int i = 0; i < nrQueries; i++) {
            Collection<ResultTuple> tuples = batchQueryJoinMultiTracker.result[i].getTuples();
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
