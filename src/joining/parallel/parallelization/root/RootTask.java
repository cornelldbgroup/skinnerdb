package joining.parallel.parallelization.root;

import joining.parallel.join.SPJoin;
import joining.parallel.uct.SPNode;
import joining.result.ResultTuple;
import joining.uct.SelectionPolicy;
import preprocessing.Context;
import query.QueryInfo;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The thread task for root parallelization.
 * In the task, a thread will run learning samples
 * and collect results that satisfy with predicates.
 * Rewards will be updated in different local UCT trees for threads.
 *
 * @author Ziyun Wei
 */
public class RootTask implements Callable<RootResult> {
    /**
     * Query to process.
     */
    private final QueryInfo query;
    /**
     * Query processing context.
     */
    private final Context context;
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
     * @param spJoin        join operator.
     * @param finish        the centralized finish flag.
     */
    public RootTask(QueryInfo query, Context context, SPJoin spJoin, AtomicBoolean finish) {
        this.query = query;
        this.context = context;
        this.spJoin = spJoin;
        this.finish = finish;
    }
    @Override
    public RootResult call() throws Exception {
        long timer1 = System.currentTimeMillis();
        int tid = spJoin.tid;
        int[] joinOrder = new int[query.nrJoined];
        long roundCtr = 0;
        // Get default action selection policy
        SelectionPolicy policy = SelectionPolicy.UCB1;
        // Initialize UCT join order search tree
        SPNode root = new SPNode(0, query, true, spJoin.nrThreads);
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
                if (!finish.get()) {
                    finish.set(true);
                }
            }
//            joinOp.writeLog("Episode Time: " + (end - start) + "\tReward: " + reward);
        }
        // Materialize result table
        long timer2 = System.currentTimeMillis();
        spJoin.roundCtr = roundCtr;
        System.out.println("Thread " + tid + " " + (timer2 - timer1) + "\t Round: " + roundCtr);
        Set<ResultTuple> tuples = spJoin.result.tuples;
        return new RootResult(tuples, spJoin.logs, tid);
    }
}
