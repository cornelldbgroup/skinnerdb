package joining.parallel.parallelization.tree;

import joining.parallel.join.SPJoin;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The thread task for tree parallelization.
 * In the task, a thread will run learning samples
 * and collect results that satisfy with predicates.
 * Rewards will be updated in a centralized UCT tree.
 *
 * @author Ziyun Wei
 */
public class TreeTask implements Callable<TreeResult> {
    /**
     * Query to process.
     */
    private final QueryInfo query;
    /**
     * Query processing context.
     */
    private final Context context;
    /**
     * The root of centralized UCT tree.
     */
    private SPNode root;
    /**
     * Search parallelization operators.
     */
    private final SPJoin spJoin;
    /**
     * Atomic finish flags.
     */
    private final AtomicBoolean finish;

    /**
     * Initialization of a root task
     *
     * @param query         query to process.
     * @param context       query processing context.
     * @param root          the root of single uct tree
     * @param spJoin        join operator.
     * @param finish        the centralized finish flag.
     */
    public TreeTask(QueryInfo query, Context context, SPNode root, SPJoin spJoin, AtomicBoolean finish) {
        this.query = query;
        this.context = context;
        this.root = root;
        this.spJoin = spJoin;
        this.finish = finish;
    }
    @Override
    public TreeResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int tid = spJoin.tid;
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize counter until scale down
        long nextScaleDown = 1;
        // Initialize counter until memory loss
        long nextForget = 1;
        // Initialize plot counter
        int plotCtr = 0;
        // Iterate until join result was generated
        double accReward = 0;
        while (!finish.get()) {
            ++roundCtr;
            double reward;
            reward = root.sample(roundCtr, joinOrder, spJoin, policy, false);
            // Count reward except for final sample
            if (!spJoin.isFinished()) {
                accReward += reward;
            }
            // broadcasting the finished plan.
            else {
                if (finish.compareAndSet(false, true)) {
                    System.out.println("Finish id: " + tid + "\t" + Arrays.toString(joinOrder) + "\t" + roundCtr);
                    spJoin.roundCtr = roundCtr;
                }
                break;
            }
//            joinOp.writeLog("Episode Time: " + (end - start) + "\tReward: " + reward);
        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        System.out.println("Thread " + tid + " " + (timer2 - timer1) + "\t Round: " + roundCtr);
        Set<ResultTuple> tuples = spJoin.result.tuples;
        return new TreeResult(tuples, spJoin.logs, tid);
    }
}
