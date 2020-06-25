package joining.tasks;

import config.JoinConfig;
import config.LoggingConfig;
import config.ParallelConfig;
import joining.join.DPJoin;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import joining.uct.UctNode;

import query.QueryInfo;
import visualization.TreePlotter;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The worker task for data parallel.
 * In the task, a worker will run learning samples
 * and collect results within the partition of the split table.
 *
 * @author Ziyun Wei
 */
public class DPTask implements Callable<Set<ResultTuple>> {
    /**
     * Query to process.
     */
    private final QueryInfo query;
    /**
     * The root of parallel UCT tree.
     */
    private final UctNode root;
    /**
     * Join executor.
     */
    private final DPJoin joinOp;
    /**
     * The flag that represents the termination signal.
     */
    private final AtomicBoolean joinFinished;
    /**
     * The coordinator that decides the choice of split table.
     */
    private final SplitTableCoordinator coordinator;

    /**
     * Initialization of worker task.
     *
     * @param query         query to process
     * @param root          root of UCT tree
     * @param joinOp        join operator
     * @param joinFinished  finish flag
     * @param coordinator   split table coordinator
     */
    public DPTask(QueryInfo query, UctNode root, DPJoin joinOp,
                        AtomicBoolean joinFinished, SplitTableCoordinator coordinator) {
        this.query = query;
        this.root = root;
        this.joinOp = joinOp;
        this.joinFinished = joinFinished;
        this.coordinator = coordinator;
    }

    @Override
    public Set<ResultTuple> call() throws Exception {
        // Initialize counters and variables
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        int tid = joinOp.tid;
        // copy UCT root
        UctNode root = this.root;
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
        while (!joinFinished.get()) {
            ++roundCtr;
            joinOp.roundCtr = (int) roundCtr;
            // retrieve the split table from the coordinator
            int splitTable = coordinator.getSplitTable();
            double reward;
            joinOp.splitTable = splitTable;
            if (splitTable != -1) {
                joinOrder = coordinator.getJoinOrder();
                reward = joinOp.execute(joinOrder);
                coordinator.optimizeSplitTable(joinOp);
            }
            else {
                reward = root.sample(roundCtr, joinOrder, policy);
            }
            // Count reward except for final sample
            if (!joinOp.isFinished()) {
                accReward += reward;
                maxReward = Math.max(reward, maxReward);
            }
            else {
                splitTable = joinOp.splitTable;
                if (coordinator.firstFinished
                        .compareAndSet(false, true)) {
                    System.out.println(tid + " finishes with: " +
                            Arrays.toString(joinOrder) + " splitting " + splitTable);
                }
                boolean isFinished = coordinator.setAndCheckFinished(tid, splitTable);
                if (isFinished) {
                    joinFinished.set(true);
                    break;
                }
            }

            // Consider memory loss
            if (JoinConfig.FORGET && roundCtr == nextForget) {
                root = new UctNode(roundCtr, query, true, joinOp);
                nextForget *= 10;
            }
            // Generate logging entries if activated
            log("Selected join order " + Arrays.toString(joinOrder));
            log("Obtained reward:\t" + reward);
            log("Table offsets:\t" + Arrays.toString(joinOp.tracker.tableOffset));
            log("Table cardinalities:\t" + Arrays.toString(joinOp.cardinalities));
            // Generate plots if activated
            if (query.explain && plotCtr < query.plotAtMost &&
                    roundCtr % query.plotEvery == 0) {
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

        return joinOp.result.tuples;
    }

    /**
     * Print out log entry if the maximal number of log
     * entries has not been reached yet.
     *
     * @param logEntry	log entry to print
     */
    static void log(String logEntry) {
        if (LoggingConfig.PRINT_JOIN_LOGS) {
            System.out.println(logEntry);
        }
    }
}
