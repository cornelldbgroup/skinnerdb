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

public class JoinMicroThread implements Callable<Double> {
    final AtomicBoolean isFinished;
    final AtomicInteger budget;
    final ConcurrentLinkedQueue<int[]>[] tasks;
    final LinkedList<int[]> tasksCache;
    final List<JoinMicroThread> threads;
    public final Queue<ResultTuple> tuples = new LinkedList<>();
    public boolean isNull;
    int fetch = 0;
    int put = 0;
    int nrNull = 0;
    final ParaJoin joinOp;

    public JoinMicroThread(AtomicBoolean isFinished, AtomicInteger budget,
                           ConcurrentLinkedQueue<int[]>[] tasks,
                           List<JoinMicroThread> threads, ParaJoin joinOp) {
        this.isFinished = isFinished;
        this.budget = budget;
        this.tasks = tasks;
        // Local tasks
        this.tasksCache = new LinkedList<>();
        this.threads = threads;
        this.joinOp = joinOp;
    }

    @Override
    public Double call() throws Exception {
        double reward = 0;
        int[] task;
        int remainingBudget = budget.get();
        JoinMicroTask taskWorker = new JoinMicroTask(joinOp);
        while (!isFinished.get()) {
            task = fetch();
            if (task != null) {
                isNull = false;
                fetch++;
//                reward += task.compute(tasksCache);
                int steps = taskWorker.computeInBatch(task, tuples);
                remainingBudget = budget.updateAndGet((x) -> x-steps);
//                System.out.println("Task step: " + steps);
//                reward = task.compute();
//                remainingBudget = budget.getAndDecrement();

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

    public int[] fetch() {
        int[] task = null;
        int nrJoined = tasks.length;
        for (int taskCtr = nrJoined - 1; taskCtr >= 0; taskCtr--) {
            task = tasks[taskCtr % nrJoined].poll();
            if (task != null) {
                break;
            }
        }
        return task;
    }

    public boolean checkFinished() {
        for (JoinMicroThread thread: threads) {
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
