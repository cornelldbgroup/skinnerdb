package joining.parallelization.lockfree;

import config.JoinConfig;
import joining.join.DPJoin;
import joining.parallelization.EndPlan;
import joining.result.ResultTuple;
import joining.uct.DPNode;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class LockFreeTask implements Callable<LockFreeResult>{

    private final QueryInfo query;
    private final Context context;
    private DPNode root;
    private final EndPlan endPlan;
    private final AtomicBoolean finish;
    private final ReentrantLock lock;
    private final DPJoin joinOp;


    public LockFreeTask(QueryInfo query, Context context, DPNode root, EndPlan endPlan,
                        AtomicBoolean finish, ReentrantLock lock, DPJoin dpJoin) {
        this.query = query;
        this.context = context;
        this.root = root;
        this.endPlan = endPlan;
        this.finish = finish;
        this.lock = lock;
        this.joinOp = dpJoin;
    }

    @Override
    public LockFreeResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        // Initialize counters and variables
        int tid = joinOp.tid;
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
                            roundCtr, joinOrder, joinOp,
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

        while (true) {
            long start = System.currentTimeMillis();
            ++roundCtr;
            double reward;
            int finalTable = endPlan.getSplitTable();
            if (finalTable != -1) {
                joinOrder = endPlan.getJoinOrder();
//                joinOp.budget = Integer.MAX_VALUE;
                reward = joinOp.execute(joinOrder, finalTable, (int) roundCtr);
            }
            else {
                reward = root.sample(roundCtr, joinOrder, joinOp, policy);
            }
            // Count reward except for final sample
            if (!joinOp.isFinished()) {
                accReward += reward;
                maxReward = Math.max(reward, maxReward);
            }
            // broadcasting the finished plan.
            else {
                int splitTable = joinOp.lastTable;
                if (!finish.get()) {
                    lock.lock();
                    if (!finish.get()) {
                        System.out.println(tid + " shared: " + Arrays.toString(joinOrder) + " splitting " + splitTable);
                        endPlan.setJoinOrder(joinOrder);
                        endPlan.setSplitTable(splitTable);
                        finish.set(true);
                    }
                    lock.unlock();
                }
                if (splitTable == endPlan.getSplitTable()) {
                    break;
                } else {
                    System.out.println(tid + ": bad restart");
                }
            }
            long end = System.currentTimeMillis();
            joinOp.writeLog("Episode Time: " + (end - start));

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
//            if (JoinConfig.FORGET && roundCtr==nextForget) {
//                root = new DPNode(roundCtr, query, true, 0);
//                nextForget *= 10;
//            }
            // Generate logging entries if activated
//            log("Selected join order " + Arrays.toString(joinOrder));
//            log("Obtained reward:\t" + reward);
//            log("Table offsets:\t" + Arrays.toString(joinOp.tracker.tableOffset));
//            log("Table cardinalities:\t" + Arrays.toString(joinOp.cardinalities));
        }

        // Update statistics
//        JoinStats.nrSamples = roundCtr;
//        JoinStats.avgReward = accReward/roundCtr;
//        JoinStats.maxReward = maxReward;
//        JoinStats.totalWork = 0;
//        for (int tableCtr=0; tableCtr<query.nrJoined; ++tableCtr) {
//            if (tableCtr == joinOrder[0]) {
//                JoinStats.totalWork += 1;
//            } else {
//                JoinStats.totalWork += Math.max(
//                        joinOp.tracker.tableOffset[tableCtr],0)/
//                        (double)joinOp.cardinalities[tableCtr];
//            }
//        }
//        // Output final stats if join logging enabled
//        if (LoggingConfig.MAX_JOIN_LOGS > 0) {
//            System.out.println("Exploration weight:\t" +
//                    JoinConfig.EXPLORATION_WEIGHT);
//            System.out.println("Nr. rounds:\t" + roundCtr);
//            System.out.println("Table offsets:\t" +
//                    Arrays.toString(joinOp.tracker.tableOffset));
//            System.out.println("Table cards.:\t" +
//                    Arrays.toString(joinOp.cardinalities));
//        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        System.out.println("Thread " + tid + " " + (timer2 - timer1) + "\t Round: " + roundCtr);
        Collection<ResultTuple> tuples = joinOp.result.getTuples();
        return new LockFreeResult(tuples, joinOp.logs, tid);
    }

    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry	log entry to print
     */
    static void log(String logEntry) {
        System.out.println(logEntry);
    }
}
