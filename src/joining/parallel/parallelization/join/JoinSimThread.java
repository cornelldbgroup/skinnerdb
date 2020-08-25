package joining.parallel.parallelization.join;


import joining.parallel.join.ParaJoin;
import joining.result.ResultTuple;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class JoinSimThread implements Callable<Double> {
    final AtomicBoolean isFinished;
    final AtomicInteger budget;
    final ConcurrentLinkedQueue<List<int[]>> tasks;
    final LinkedList<int[]> tasksCache;
    final List<JoinSimThread> threads;
    public final Queue<ResultTuple> tuples = new LinkedList<>();
    public boolean isNull;
    int fetch = 0;
    int nrNull = 0;
    final ParaJoin joinOp;
    final int localBudget;

    public JoinSimThread(AtomicBoolean isFinished, AtomicInteger budget,
                         ConcurrentLinkedQueue<List<int[]>> tasks,
                         List<JoinSimThread> threads, ParaJoin joinOp, int localBudget) {
        this.isFinished = isFinished;
        this.budget = budget;
        this.tasks = tasks;
        // Local tasks
        this.tasksCache = new LinkedList<>();
        this.threads = threads;
        this.joinOp = joinOp;
        this.localBudget = localBudget;
    }

    @Override
    public Double call() throws Exception {
        double reward = 0;
        List<int[]> task;
        int remainingBudget = budget.get();
        JoinMicroTask taskWorker = new JoinMicroTask(joinOp);
        taskWorker.tid = -1;
        while (!isFinished.get()) {
            task = tasks.poll();
            if (task != null) {
                isNull = false;
                fetch++;
                int steps = taskWorker.seqExecute(task);
                remainingBudget = budget.updateAndGet((x) -> x-steps);

            }
            else {
                isNull = true;
                nrNull++;
                // All threads finish join the query
                if (checkFinished()) {
                    isFinished.set(true);
                    return reward;
                }
            }
            // No remaining budget
            if (remainingBudget <= 0) {
                isFinished.set(true);
            }
        }
        return reward;
    }
    public boolean checkFinished() {
        for (JoinSimThread thread: threads) {
            if (!thread.isNull) {
                return false;
            }
        }
        return true;
    }

    public void reset(int budget) {
        this.budget.set(budget);
        isFinished.set(false);
    }
}
