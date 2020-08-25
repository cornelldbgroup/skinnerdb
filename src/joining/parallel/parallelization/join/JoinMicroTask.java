package joining.parallel.parallelization.join;

import config.ParallelConfig;
import config.PreConfig;
import expressions.compilation.KnaryBoolEval;
import joining.parallel.join.JoinPartitionIndexWrapper;
import joining.parallel.join.ParaJoin;
import joining.parallel.plan.LeftDeepPartitionPlan;
import joining.result.ResultTuple;
import org.jetbrains.annotations.NotNull;
import predicate.NonEquiNode;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JoinMicroTask implements Comparable<JoinMicroTask> {
    public final int[] tupleIndices;
    public final int joinIndex;
    final ParaJoin joinOp;
    public int[] tail;
    public int tid;
    public final int nrThreads = ParallelConfig.EXE_THREADS;


    public JoinMicroTask(int[] tupleIndices, int joinIndex,
                         ParaJoin joinOp, int tid) {
        this.tupleIndices = tupleIndices;
        this.joinIndex = joinIndex;
        this.joinOp = joinOp;
        this.tid = tid;
    }

    public JoinMicroTask(ParaJoin joinOp) {
        this.tupleIndices = new int[joinOp.nrJoined];
        this.joinIndex = -1;
        this.joinOp = joinOp;
    }

    public int evaluate(int[] task, Queue<ResultTuple> tuples) {
        int localBudget = 100;
        int nrJoined = joinOp.nrJoined;
        LeftDeepPartitionPlan plan = joinOp.plan;
        int[] order = plan.joinOrder.order;
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        // Copy task information
        int curIndex = task[nrJoined];
        tid = task[nrJoined + 1];
        System.arraycopy(task, 0, tupleIndices, 0, nrJoined);
        int curTable = order[curIndex];
        int curSteps = 0;
        int nrBranches = ParallelConfig.EXE_THREADS;

        // Integrate table offset
        int[][] nextTasks = new int[nrJoined][nrJoined+2];
        for (int[] nextTask: nextTasks) {
            nextTask[0] = -1;
        }
        while (curSteps < localBudget) {
            // TODO: enable offset
//            int offset = joinOp.offsets[curTable];
//            tupleIndices[curTable] = Math.max(offset, tupleIndices[curTable]);
//            System.out.println(Arrays.toString(tupleIndices) + " " + curIndex);
            curSteps++;
            // Find rightmost admissible join index
            if (evaluate(joinIndices.get(curIndex),
                    applicablePreds.get(curIndex), tupleIndices, curTable)) {
                if (curIndex == order.length - 1) {
                    tuples.add(new ResultTuple(tupleIndices));
//                    joinOp.logs.add(Arrays.toString(tupleIndices));
                    int nextIndex = proposeNext(order, joinIndices, curIndex, tupleIndices, tid);
                    if (nextIndex < curIndex && curSteps < localBudget) {
                        while (nextIndex >= 0 && nextTasks[order[nextIndex]][0] < 0) {
                            nextIndex--;
                        }
                        if (nextIndex < 0) {
                            break;
                        }
                        else {
                            int nextTable = order[nextIndex];
                            int[] taskInfo = nextTasks[nextTable];
                            System.arraycopy(taskInfo,
                                    0, tupleIndices, 0, nrJoined);
                            curIndex = nextIndex;
                            curTable = nextTable;
                            tid = taskInfo[nrJoined + 1];
                            nextTasks[nextTable][0] = -1;
                        }
                    }

                }
                else {
                    if (tid == -1) {
                        List<int[]> downTasks = proposeTasks(order, joinIndices,
                                curIndex, tupleIndices, nrBranches, localBudget);
                        if (downTasks == null) {
                            int[] downTuples = new int[nrJoined + 2];
                            System.arraycopy(tupleIndices, 0, downTuples, 0, nrJoined);
                            downTuples[curTable] += 1;
                            downTuples[nrJoined] = curIndex;
                            downTuples[nrJoined + 1] = -2;
                            nextTasks[order[curIndex]] = downTuples;
                        }
                        else {
                            if (downTasks.size() == nrBranches) {
                                nextTasks[order[curIndex]] = downTasks.remove(nrBranches - 1);
                            }
                        }
                    }
                    else {
                        int[] downTuples = new int[nrJoined + 2];
                        System.arraycopy(tupleIndices, 0, downTuples, 0, nrJoined);
                        int nextIndex = proposeNext(order, joinIndices, curIndex, downTuples, tid);
                        if (nextIndex == curIndex) {
                            downTuples[nrJoined] = curIndex;
                            downTuples[nrJoined + 1] = tid;
                            nextTasks[order[nextIndex]] = downTuples;
                        }
                    }
                    curIndex++;
                    curTable = order[curIndex];
                    tid = -1;
                }
            }
            else {
                boolean runNext = true;
                if (tid == -1) {
                    List<int[]> downTasks = proposeTasks(order, joinIndices,
                            curIndex, tupleIndices, nrBranches, localBudget);
                    if (downTasks == null) {
                        tid = -2;
                        runNext = true;
                    }
                    else {
                        if (downTasks.size() == nrBranches && curSteps < localBudget) {
                            int[] taskInfo = downTasks.remove(nrBranches - 1);
                            System.arraycopy(taskInfo, 0, tupleIndices, 0, nrJoined);
                            runNext = false;
                            tid = taskInfo[nrJoined + 1];
                        }
                    }
                }
                if (runNext) {
                    int nextIndex = proposeNext(order, joinIndices, curIndex, tupleIndices, tid);
                    if (nextIndex < curIndex && curSteps < localBudget) {
                        while (nextIndex >= 0 && nextTasks[order[nextIndex]][0] < 0) {
                            nextIndex--;
                        }
                        if (nextIndex < 0) {
                            break;
                        }
                        else {
                            int nextTable = order[nextIndex];
                            int[] taskInfo = nextTasks[nextTable];
                            System.arraycopy(taskInfo,
                                    0, tupleIndices, 0, nrJoined);
                            curIndex = nextIndex;
                            curTable = nextTable;
                            tid = taskInfo[nrJoined + 1];
                            nextTasks[nextTable][0] = -1;
                        }
                    }
                }
            }

        }
        return curSteps;
    }


    public double compute() {
        LeftDeepPartitionPlan plan = joinOp.plan;
        int[] order = plan.joinOrder.order;
        int nrBranches = ParallelConfig.EXE_THREADS;
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        int nextTable = order[joinIndex];
        // Integrate table offset
        int offset = joinOp.offsets[nextTable];
        tupleIndices[nextTable] = Math.max(offset, tupleIndices[nextTable]);
        int curIndex = joinIndex;
        int curTable = order[curIndex];
        int nrJoined = joinOp.nrJoined;
        // Find rightmost admissible join index
        if (evaluate(joinIndices.get(curIndex),
                applicablePreds.get(curIndex), tupleIndices, curTable)) {
            if(curIndex == order.length - 1) {
                joinOp.tuples.add(new ResultTuple(tupleIndices));
            }
            else {
                curIndex++;
                curTable = order[curIndex];
                int[] rightTuples = new int[nrJoined + 2];
                System.arraycopy(tupleIndices, 0, rightTuples, 0, nrJoined);
                rightTuples[nrJoined] = curIndex;
                rightTuples[nrJoined + 1] = -1;
                joinOp.curTasks[curTable].add(rightTuples);
                joinOp.createdTasks[curTable].add(rightTuples);
            }
        }
        if (tid < 0) {
            int[] downTuples = tail == null ? tupleIndices : tail;
            List<int[]> downTasks = proposeTasks(order, joinIndices, joinIndex, downTuples, nrBranches, 0);
            if (downTasks.size() > 0) {
                for (int[] downTask: downTasks) {
                    joinOp.curTasks[nextTable].add(downTask);
                    joinOp.createdTasks[nextTable].add(downTask);
                }
            }
        }
        return joinOp.rewardPerTable[nextTable];
    }

    public int computeInBatch(int[] task, Queue<ResultTuple> tuples) {
        int localBudget = 100;
        int nrJoined = joinOp.nrJoined;
        LeftDeepPartitionPlan plan = joinOp.plan;
        int[] order = plan.joinOrder.order;
        List<List<JoinPartitionIndexWrapper>> joinIndices = plan.joinIndices;
        List<List<NonEquiNode>> applicablePreds = plan.nonEquiNodes;
        // Copy task information
        int curIndex = task[nrJoined];
        tid = task[nrJoined + 1];
        System.arraycopy(task, 0, tupleIndices, 0, nrJoined);
        int curTable = order[curIndex];
        int curSteps = 0;
        int nrBranches = ParallelConfig.EXE_THREADS;

        // Integrate table offset
        int[][] nextTasks = new int[nrJoined][nrJoined+2];
        for (int[] nextTask: nextTasks) {
            nextTask[0] = -1;
        }
        while (curSteps < localBudget) {
            // TODO: enable offset
//            int offset = joinOp.offsets[curTable];
//            tupleIndices[curTable] = Math.max(offset, tupleIndices[curTable]);
//            System.out.println(Arrays.toString(tupleIndices) + " " + curIndex);
            curSteps++;
            // Find rightmost admissible join index
            if (evaluate(joinIndices.get(curIndex),
                    applicablePreds.get(curIndex), tupleIndices, curTable)) {
                if (curIndex == order.length - 1) {
                    tuples.add(new ResultTuple(tupleIndices));
//                    joinOp.logs.add(Arrays.toString(tupleIndices));
                    int nextIndex = proposeNext(order, joinIndices, curIndex, tupleIndices, tid);
                    if (nextIndex < curIndex && curSteps < localBudget) {
                        while (nextIndex >= 0 && nextTasks[order[nextIndex]][0] < 0) {
                            nextIndex--;
                        }
                        if (nextIndex < 0) {
                            break;
                        }
                        else {
                            int nextTable = order[nextIndex];
                            int[] taskInfo = nextTasks[nextTable];
                            System.arraycopy(taskInfo,
                                    0, tupleIndices, 0, nrJoined);
                            curIndex = nextIndex;
                            curTable = nextTable;
                            tid = taskInfo[nrJoined + 1];
                            nextTasks[nextTable][0] = -1;
                        }
                    }
                    else if (nextIndex == curIndex && curSteps == localBudget) {
                        int[] taskInfo = new int[nrJoined + 2];
                        System.arraycopy(tupleIndices, 0, taskInfo, 0, nrJoined);
                        taskInfo[nrJoined] = curIndex;
                        taskInfo[nrJoined + 1] = tid;
                        joinOp.curTasks[curTable].add(taskInfo);
                        joinOp.createdTasks[curTable].add(taskInfo);
                    }
                }
                else {
                    if (tid == -1) {
                        List<int[]> downTasks = proposeTasks(order, joinIndices,
                                curIndex, tupleIndices, nrBranches, localBudget);
                        if (downTasks == null) {
                            int[] downTuples = new int[nrJoined + 2];
                            System.arraycopy(tupleIndices, 0, downTuples, 0, nrJoined);
                            downTuples[curTable] += 1;
                            downTuples[nrJoined] = curIndex;
                            downTuples[nrJoined + 1] = -2;
                            nextTasks[order[curIndex]] = downTuples;
                        }
                        else {
                            if (downTasks.size() == nrBranches) {
                                nextTasks[order[curIndex]] = downTasks.remove(nrBranches - 1);
                            }
                            for (int[] downTask: downTasks) {
                                joinOp.curTasks[curTable].add(downTask);
                                joinOp.createdTasks[curTable].add(downTask);
                            }
                        }
                    }
                    else {
                        int[] downTuples = new int[nrJoined + 2];
                        System.arraycopy(tupleIndices, 0, downTuples, 0, nrJoined);
                        int nextIndex = proposeNext(order, joinIndices, curIndex, downTuples, tid);
                        if (nextIndex == curIndex) {
                            downTuples[nrJoined] = curIndex;
                            downTuples[nrJoined + 1] = tid;
                            nextTasks[order[nextIndex]] = downTuples;
                        }
                    }
                    curIndex++;
                    curTable = order[curIndex];
                    tid = -1;
                    if (curSteps == localBudget) {
                        int[] taskInfo = new int[nrJoined + 2];
                        System.arraycopy(tupleIndices, 0, taskInfo, 0, nrJoined);
                        taskInfo[nrJoined] = curIndex;
                        taskInfo[nrJoined + 1] = tid;
                        joinOp.curTasks[curTable].add(taskInfo);
                        joinOp.createdTasks[curTable].add(taskInfo);
                    }
                }
            }
            else {
                boolean runNext = true;
                if (tid == -1) {
                    List<int[]> downTasks = proposeTasks(order, joinIndices,
                            curIndex, tupleIndices, nrBranches, localBudget);
                    if (downTasks == null) {
                        tid = -2;
                        runNext = true;
                    }
                    else {
                        if (downTasks.size() == nrBranches && curSteps < localBudget) {
                            int[] taskInfo = downTasks.remove(nrBranches - 1);
                            System.arraycopy(taskInfo, 0, tupleIndices, 0, nrJoined);
                            runNext = false;
                            tid = taskInfo[nrJoined + 1];
                        }
                        for (int[] downTask: downTasks) {
                            joinOp.curTasks[curTable].add(downTask);
                            joinOp.createdTasks[curTable].add(downTask);
                        }
                    }
                }
                if (runNext) {
                    int nextIndex = proposeNext(order, joinIndices, curIndex, tupleIndices, tid);
                    if (nextIndex < curIndex && curSteps < localBudget) {
                        while (nextIndex >= 0 && nextTasks[order[nextIndex]][0] < 0) {
                            nextIndex--;
                        }
                        if (nextIndex < 0) {
                            break;
                        }
                        else {
                            int nextTable = order[nextIndex];
                            int[] taskInfo = nextTasks[nextTable];
                            System.arraycopy(taskInfo,
                                    0, tupleIndices, 0, nrJoined);
                            curIndex = nextIndex;
                            curTable = nextTable;
                            tid = taskInfo[nrJoined + 1];
                            nextTasks[nextTable][0] = -1;
                        }
                    }
                    else if (nextIndex == curIndex && curSteps == localBudget) {
                        int[] taskInfo = new int[nrJoined + 2];
                        System.arraycopy(tupleIndices, 0, taskInfo, 0, nrJoined);
                        taskInfo[nrJoined] = curIndex;
                        taskInfo[nrJoined + 1] = tid;
                        joinOp.curTasks[curTable].add(taskInfo);
                        joinOp.createdTasks[curTable].add(taskInfo);
                    }
                }
            }

        }
        // Store remaining tasks
        for (int table = 0; table < nrJoined; table++) {
            int[] taskInfo = nextTasks[table];
            if (taskInfo[0] >= 0) {
                joinOp.curTasks[table].add(taskInfo);
                joinOp.createdTasks[table].add(taskInfo);
            }
        }
        return curSteps;
    }

    /**
     * Evaluates list of given predicates on current tuple
     * indices and returns true iff all predicates evaluate
     * to true.
     *
     * @param indexWrappers
     * @param tupleIndices
     * @return
     */
    boolean evaluate(List<JoinPartitionIndexWrapper> indexWrappers, List<NonEquiNode> preds,
                            int[] tupleIndices, int nextTable) {
        boolean isSplit = tid >= 0;
        boolean first = true;
        if (indexWrappers.isEmpty() && preds.isEmpty()) {
            if (isSplit && tupleIndices[nextTable] % nrThreads != tid) {
                return false;
            }
        }
        for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
            if (first && isSplit) {
                if (!wrapper.evaluateInScope(tupleIndices, tid)) {
                    return false;
                }
            }
            else {
                if (!wrapper.evaluate(tupleIndices)) {
                    return false;
                }
            }
            first = false;
        }
        // Evaluate non-equi join predicates
        for (NonEquiNode pred : preds) {
            if (!pred.evaluate(tupleIndices, nextTable,
                    joinOp.cardinalities[nextTable])) {
                return false;
            }
        }
        return true;
    }
    /**
     * Propose next tuple index to consider, based on a set of
     * indices on the join column.
     *
     * @param indexWrappersList	list of join index wrappers
     * @param tupleIndices	current tuple indices
     * @return				next join index
     */
    int proposeNext(int[] joinOrder, List<List<JoinPartitionIndexWrapper>> indexWrappersList,
                           int curIndex, int[] tupleIndices, int tid) {
        int nextTable = joinOrder[curIndex];
        int[] cardinalities = joinOp.cardinalities;
        int nextCardinality = cardinalities[nextTable];
//        int nrBranches = ParallelConfig.BRANCH_FACTOR;
        int nrBranches = ParallelConfig.EXE_THREADS;
        boolean isSplit = tid >= 0;
        List<JoinPartitionIndexWrapper> indexWrappers = indexWrappersList.get(curIndex);
        // If there is no equi-predicates.
        if (indexWrappers.isEmpty()) {
            if (isSplit) {
                int jump = (nrThreads + tid - tupleIndices[nextTable] % nrThreads) % nrThreads;
                jump = jump == 0 ? nrThreads : jump;
                tupleIndices[nextTable] += jump;
            }
            else {
                tupleIndices[nextTable]++;
            }
        }
        else {
            boolean first = true;
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                if (!first) {
                    if (wrapper.evaluate(tupleIndices)) {
                        continue;
                    }
                }
                int nextRaw = first && isSplit ? wrapper.nextIndexInScope(tupleIndices, tid, null):
                        wrapper.nextIndex(tupleIndices, null);
                if (nextRaw < 0 || nextRaw == nextCardinality) {
                    tupleIndices[nextTable] = nextCardinality;
                    break;
                }
                else {
                    tupleIndices[nextTable] = nextRaw;
                }
                first = false;
            }
        }
        // Have reached end of current table? -> we backtrack.
        while (tupleIndices[nextTable] >= nextCardinality) {
            tupleIndices[nextTable] = 0;
            --curIndex;
            if (curIndex < 0) {
                break;
            }
            nextTable = joinOrder[curIndex];
            nextCardinality = cardinalities[nextTable];
            tupleIndices[nextTable] += 1;
        }
        return curIndex;
    }

    List<JoinMicroTask> proposeTasks(int[] joinOrder, List<List<JoinPartitionIndexWrapper>> indexWrappersList,
                    int curIndex, int[] tupleIndices, int factor) {
        int nextTable = joinOrder[curIndex];
        int[] cardinalities = joinOp.cardinalities;
        int nextCardinality = cardinalities[nextTable];
        List<JoinMicroTask> downTasks = new ArrayList<>(factor);
        List<JoinPartitionIndexWrapper> indexWrappers = indexWrappersList.get(curIndex);
        // If there is no equi-predicates.
        if (indexWrappers.isEmpty()) {
            int end = Math.min(nextCardinality - tupleIndices[nextTable] - 1, factor);
            for (int taskCtr = 0; taskCtr < end; taskCtr++) {
                int[] downTupleIndices = tupleIndices.clone();
                downTupleIndices[nextTable] += (taskCtr + 1);
                downTasks.add(new JoinMicroTask(downTupleIndices, curIndex, joinOp, taskCtr));
            }
        }
        else {
            int[] downTupleIndices = tupleIndices.clone();
            boolean isFinished = false;
            for (int taskCtr = 0; taskCtr < factor; taskCtr++) {
                boolean first = true;
                for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                    if (!first) {
                        if (wrapper.evaluate(downTupleIndices)) {
                            continue;
                        }
                    }
                    int nextRaw = wrapper.nextIndex(downTupleIndices, null);
                    if (nextRaw >= 0 && nextRaw < nextCardinality) {
                        downTupleIndices[nextTable] = nextRaw;
                        downTasks.add(new JoinMicroTask(downTupleIndices.clone(),
                                curIndex, joinOp, taskCtr));
                    }
                    else {
                        isFinished = true;
                        break;
                    }
                    first = false;
                }
                if (isFinished) {
                    break;
                }
            }
        }
        return downTasks;
    }

    List<int[]> proposeTasks(int[] joinOrder, List<List<JoinPartitionIndexWrapper>> indexWrappersList,
                                     int curIndex, int[] tupleIndices, int factor, int localBudget) {
        int nextTable = joinOrder[curIndex];
        int[] cardinalities = joinOp.cardinalities;
        int nextCardinality = cardinalities[nextTable];
        List<int[]> downTasks = new ArrayList<>(factor);
        List<JoinPartitionIndexWrapper> indexWrappers = indexWrappersList.get(curIndex);
        int indexSize = Integer.MAX_VALUE;
        int nrJoined = tupleIndices.length;
        // If there is no equi-predicates.
        if (indexWrappers.isEmpty()) {
            indexSize = nextCardinality;
        }
        else {
            for (JoinPartitionIndexWrapper wrapper : indexWrappers) {
                indexSize = Math.min(indexSize, wrapper.nrIndexed(tupleIndices));
            }
        }
        // enough tasks
        if (indexSize >= factor * localBudget) {
            for (int taskCtr = 0; taskCtr < factor; taskCtr++) {
                int[] downTupleIndices = new int[nrJoined + 2];
                System.arraycopy(tupleIndices, 0, downTupleIndices, 0, nrJoined);
                downTupleIndices[nextTable] ++;
                downTupleIndices[nrJoined] = curIndex;
                downTupleIndices[nrJoined + 1] = taskCtr;
                downTasks.add(downTupleIndices);
            }
        }
        else {
            return null;
        }
        return downTasks;
    }

    public LinkedList<int[]> seqTasks() {
        LinkedList<int[]> tasks = new LinkedList<>();
        int joinIndex = 0;
        int[] order = joinOp.plan.joinOrder.order;
        int nrJoined = order.length;
        List<List<JoinPartitionIndexWrapper>> joinIndices = joinOp.plan.joinIndices;
        List<List<NonEquiNode>> applicablePreds = joinOp.plan.nonEquiNodes;
        tid = -1;
        int nrTuples = 0;
        while (joinIndex >= 0) {
            int nextTable = order[joinIndex];
            boolean evaluation = evaluate(joinIndices.get(joinIndex), applicablePreds.get(joinIndex),
                    tupleIndices, nextTable);
            int[] newTask = new int[nrJoined + 2];
            System.arraycopy(tupleIndices, 0, newTask, 0, nrJoined);
            newTask[nrJoined] = joinIndex;
            newTask[nrJoined + 1] = evaluation ? 1 : 0;
            tasks.add(newTask);
            if (evaluation) {
                // Do we have a complete result row?
                if(joinIndex == nrJoined - 1) {
                    // Complete result row -> add to result
                    nrTuples++;
                    joinIndex = proposeNext(order, joinIndices, joinIndex, tupleIndices, -1);
                } else {
                    // No complete result row -> complete further
                    joinIndex++;
                }
            } else {
                // At least one of applicable predicates evaluates to false -
                // try next tuple in same table.
                joinIndex = proposeNext(order, joinIndices, joinIndex, tupleIndices, -1);

            }
        }
        System.out.println("Nr. Tuples: " + nrTuples);
        return tasks;
    }

    public int seqExecute(List<int[]> taskList) {
        int[] order = joinOp.plan.joinOrder.order;
        int nrJoined = order.length;
        List<List<JoinPartitionIndexWrapper>> joinIndices = joinOp.plan.joinIndices;
        List<List<NonEquiNode>> applicablePreds = joinOp.plan.nonEquiNodes;
        int[] tupleIndices = new int[nrJoined];
        for (int[] task: taskList) {
            System.arraycopy(task, 0, tupleIndices, 0, nrJoined);
            int joinIndex = task[nrJoined];
            boolean evaluation = task[nrJoined + 1] == 1;
            if (evaluation) {
                // Do we have a complete result row?
                if(joinIndex == nrJoined - 1) {
                    // Complete result row -> add to result
                    joinOp.tuples.add(new ResultTuple(tupleIndices));
                }
            }
        }
        return taskList.size();
    }

    @Override
    public int compareTo(@NotNull JoinMicroTask otherTask) {
        int[] joinOrder = joinOp.plan.joinOrder.order;
        int nrTables = joinOrder.length;
        int[] otherIndices = otherTask.tupleIndices;
        for (int table : joinOrder) {
            if (tupleIndices[table] > otherIndices[table]) {
                // this state is ahead
                return 1;
            } else if (otherIndices[table] > tupleIndices[table]) {
                // other state is ahead
                return -1;
            }
        }
        return joinIndex - otherTask.joinIndex;
    }
}
