package joining.parallel.progress;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import joining.plan.JoinOrder;
import joining.progress.State;

import java.util.Arrays;

/**
 * Keeps track of progress made in evaluating different
 * join orders for threads. Offers methods to store progress made
 * and methods to retrieve the state from which evaluation
 * for one specific join order should continue.
 *
 * @author Ziyun Wei
 */
public class ParallelProgressTracker {
    /**
     * Number of tables in the query.
     */
    private final int nrTables;
    /**
     * Stores progress made for each join order prefix.
     */
    private final ThreadProgress sharedProgress;
    /**
     * For each table the last tuple that was fully treated.
     */
    public final int[] tableOffset;
    /**
     * number of threads
     */
    private final int nrThreads;
    /**
     * Number of ways to split a table.
     */
    private final int nrSplits;
    /**
     * record previous split key for each join order for each thread
     */
    private IntIntMap[] previousSplitTable;
    /**
     * offsets for different split tables and different threads
     */
    private int[][][] tableOffsetMaps;
    /**
     * The slowest threads.
     */
    private int[] slowestThreads;
    /**
     * upper level thread
     */
    private static int THRESHOLD = Integer.MAX_VALUE;

    /**
     * Initializes pointers to child nodes.
     *
     * @param nrTables number of tables in the database
     */
    public ParallelProgressTracker(int nrTables, int nrThreads, int nrSplits) {
        this.nrTables = nrTables;
        this.nrThreads = nrThreads;
        this.nrSplits = nrSplits;
        sharedProgress = new ThreadProgress(nrTables, nrThreads, false);
        tableOffset = new int[nrTables];
        previousSplitTable = new IntIntMap[nrThreads];
        tableOffsetMaps = new int[nrThreads][nrSplits][nrTables];
        slowestThreads = new int[nrSplits];
        Arrays.fill(slowestThreads, -1);
        if (nrSplits == 0) {
            System.out.println("here");
        }
        for (int j = 0; j < nrThreads; j++) {
            previousSplitTable[j] = HashIntIntMaps.newMutableMap();
        }
    }

    /**
     * Integrates final state achieved when evaluating one specific
     * join order.
     *
     * @param joinOrder a join order evaluated with a specific time budget
     * @param state     final state achieved when evaluating join order
     * @param threadID  thread id
     */
    public boolean updateProgress(JoinOrder joinOrder, int splitKey,
                               State state, int threadID,
                               int roundCtr, int splitTable, int firstTable) {
        // Update state for all join order prefixes
        ThreadProgress curPrefixProgress = sharedProgress;
        // Initialize the time version of the state.
        int tv = 0;
        // Whether we need to check the slowest progress for the upper level.
        boolean checking = true;
        // Iterate over position in join order
        boolean slowest = !state.isFinished();
        int upperLevel = Math.min(THRESHOLD, nrTables);
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads,false);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            ThreadState tableState = curPrefixProgress.latestStates[threadID];
            if (tableState == null) {
                state.roundCtr = roundCtr;
                curPrefixProgress.latestStates[threadID] = new ThreadState(stateIndex, splitKey, roundCtr,
                        state.lastIndex, threadID, nrSplits);
            }
            else {
                curPrefixProgress.latestStates[threadID].updateProgress(state, stateIndex,
                        splitKey, roundCtr, state.lastIndex);
            }

            if (checking) {
                int slowestFlag = curPrefixProgress.isSlowest(splitKey, threadID, stateIndex);
                if (slowestFlag == 1) {
                    slowest = true;
                    checking = false;
                    slowestThreads[splitKey] = threadID;
                }
                else if (slowestFlag == -1) {
                    slowest = false;
                    checking = false;
                }
            }
        }
        // Update progress for lower level.
        if (THRESHOLD < nrTables) {
            int intermediateTable = joinOrder.order[THRESHOLD];
            ThreadProgress lowerProgress = curPrefixProgress.childNodes[intermediateTable];

            if (lowerProgress != null && lowerProgress.slowState.threadID == threadID) {
                for (int joinCtr = THRESHOLD; joinCtr < nrTables; ++joinCtr) {
                    int table = joinOrder.order[joinCtr];
                    int stateIndex = state.tupleIndices[table];
                    if (curPrefixProgress.childNodes[table] == null) {
                        curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads, true);
                    }
                    curPrefixProgress = curPrefixProgress.childNodes[table];
                    ThreadState hybridState = curPrefixProgress.slowState;
                    if (hybridState == null) {
                        curPrefixProgress.slowState = new ThreadState(state.tupleIndices[table], splitKey, roundCtr,
                                state.lastIndex, threadID, nrSplits);
                    }
                    else {
                        curPrefixProgress.slowState.updateProgress(state, stateIndex, splitKey, roundCtr,
                                state.lastIndex);
                        curPrefixProgress.slowState.threadID = threadID;
                    }
                }
            }
            else if (lowerProgress == null || slowest) {
                ThreadProgress hybridProgressRoot = new ThreadProgress(nrThreads, nrThreads, true);
                hybridProgressRoot.slowState = new ThreadState(state.tupleIndices[intermediateTable], splitKey, roundCtr,
                        state.lastIndex, threadID, nrSplits);
                ThreadProgress hybridProgress = hybridProgressRoot;

                for (int joinCtr = THRESHOLD + 1; joinCtr < nrTables; ++joinCtr) {
                    int table = joinOrder.order[joinCtr];
                    hybridProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads, true);
                    hybridProgress = hybridProgress.childNodes[table];
                    hybridProgress.slowState = new ThreadState(state.tupleIndices[table], splitKey, roundCtr,
                            state.lastIndex, threadID, nrSplits);
                }
                curPrefixProgress.childNodes[intermediateTable] = hybridProgressRoot;
            }
        }
        // Join Order Prefix
        int key = joinOrder.getPrefixKey(Math.min(2, nrTables));
        // save previous split key
        previousSplitTable[threadID].put(key, splitKey);

        // Update table offset considering last fully treated tuple
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        tableOffsetMaps[threadID][splitKey][firstTable] = lastTreatedTuple;
        int currentOffset = Integer.MAX_VALUE;
        for (int tid = 0; tid < nrThreads; tid++) {
            currentOffset = Math.min(tableOffsetMaps[tid][splitKey][firstTable], currentOffset);
        }
        tableOffset[firstTable] = Math.max(currentOffset, tableOffset[firstTable]);
        return slowest;
    }

    public void updateProgressSP(JoinOrder joinOrder,
                               State state, int threadID,
                               int roundCtr, int firstTable) {
        // Update state for all join order prefixes
        ThreadProgress curPrefixProgress = sharedProgress;
        // Initialize the time version of the state.
        int tv = 0;
        // Whether we need to check the slowest progress for the upper level.
        boolean checking = true;
        // Iterate over position in join order
        boolean slowest = !state.isFinished();
        int upperLevel = Math.min(THRESHOLD, nrTables);
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads,false);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            ThreadState tableState = curPrefixProgress.latestStates[threadID];

            if (tableState == null) {
                state.roundCtr = roundCtr;
                curPrefixProgress.latestStates[threadID] = new ThreadState(stateIndex, 0, roundCtr,
                        state.lastIndex, threadID, nrSplits);
            }
            else {
                curPrefixProgress.latestStates[threadID].updateProgress(state, stateIndex, 0, roundCtr, state.lastIndex);
            }

//            if (checking) {
//                int slowestFlag = curPrefixProgress.isSlowest(splitKey, threadID, stateIndex);
//                if (slowestFlag == 1) {
//                    slowest = true;
//                    checking = false;
//                }
//                else if (slowestFlag == -1) {
//                    slowest = false;
//                    checking = false;
//                }
//            }
        }
        // Update progress for lower level.
        if (THRESHOLD < nrTables) {
            int intermediateTable = joinOrder.order[THRESHOLD];
            ThreadProgress lowerProgress = curPrefixProgress.childNodes[intermediateTable];

            if (lowerProgress != null && lowerProgress.slowState.threadID == threadID) {
                for (int joinCtr = THRESHOLD; joinCtr < nrTables; ++joinCtr) {
                    int table = joinOrder.order[joinCtr];
                    int stateIndex = state.tupleIndices[table];
                    if (curPrefixProgress.childNodes[table] == null) {
                        curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads, true);
                    }
                    curPrefixProgress = curPrefixProgress.childNodes[table];
                    ThreadState hybridState = curPrefixProgress.slowState;
                    if (hybridState == null) {
                        curPrefixProgress.slowState = new ThreadState(state.tupleIndices[table], 0, roundCtr,
                                state.lastIndex, threadID, nrSplits);
                    }
                    else {
                        curPrefixProgress.slowState.updateProgress(state, stateIndex, 0, roundCtr, state.lastIndex);
                        curPrefixProgress.slowState.threadID = threadID;
                    }
                }
            }
            else if (lowerProgress == null || slowest) {
                ThreadProgress hybridProgressRoot = new ThreadProgress(nrThreads, nrThreads, true);
                hybridProgressRoot.slowState = new ThreadState(state.tupleIndices[intermediateTable], 0,
                        roundCtr, state.lastIndex, threadID, nrSplits);
                ThreadProgress hybridProgress = hybridProgressRoot;

                for (int joinCtr = THRESHOLD + 1; joinCtr < nrTables; ++joinCtr) {
                    int table = joinOrder.order[joinCtr];
                    hybridProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads, true);
                    hybridProgress = hybridProgress.childNodes[table];
                    hybridProgress.slowState = new ThreadState(state.tupleIndices[table], 0, roundCtr,
                            state.lastIndex, threadID, nrSplits);
                }
                curPrefixProgress.childNodes[intermediateTable] = hybridProgressRoot;
            }
        }

        // Update table offset considering last fully treated tuple
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        tableOffset[firstTable] = Math.max(lastTreatedTuple, tableOffset[firstTable]);
    }

    /**
     * Returns state from which evaluation of the given join order
     * must start in order to guarantee that all results are generated.
     *
     * @param joinOrder  a join order
     * @return start state for evaluating join order
     */
    public State continueFrom(JoinOrder joinOrder, int splitKey,
                              int threadID, boolean slowThreads) {
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        ThreadProgress curPrefixProgress = sharedProgress;
        int upperLevel = Math.min(THRESHOLD, nrTables);

        int key = joinOrder.getPrefixKey(Math.min(2, nrTables));
        int previousSplitKey = previousSplitTable[threadID].getOrDefault(key, -1);
        int slowestID = previousSplitKey >= 0 ? slowestThreads[previousSplitKey] : -1;
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null || curPrefixProgress.latestStates[threadID] == null) {
                return state;
            }
            // get a thread state
            ThreadState threadState = curPrefixProgress.latestStates[threadID];
            // if the state contains the progress for a given key
            if (threadState.hasProgress(splitKey) && (!slowThreads || previousSplitKey == splitKey)) {
                threadState.fastForward(state, table, splitKey, joinCtr);
                if (state.roundCtr < 0) {
                    return state;
                }
            }
            else {
                if (previousSplitKey >= 0) {
                    if (slowestID >= 0 && curPrefixProgress.latestStates[slowestID] != null) {
                        int slowestProgress = curPrefixProgress.latestStates[slowestID].getProgress(previousSplitKey);
                        if (slowestProgress >= 0) {
                            state.tupleIndices[table] = slowestProgress;
                        }
                    }
                    else {
                        int slowestProgress = curPrefixProgress.getSlowestProgress(previousSplitKey);
                        if (slowestProgress >= 0) {
                            state.tupleIndices[table] = slowestProgress;
                        }
                    }
                }
                else {
                    return state;
                }
            }
        }

        if (THRESHOLD >= nrTables) {
            return state;
        }

        int intermediateTable = joinOrder.order[THRESHOLD];
        if (curPrefixProgress.childNodes[intermediateTable] != null &&
                curPrefixProgress.childNodes[intermediateTable].slowState.threadID == threadID) {
            for (int joinCtr = THRESHOLD; joinCtr < nrTables; joinCtr++) {
                int table = order[joinCtr];
                curPrefixProgress = curPrefixProgress.childNodes[table];
                if (curPrefixProgress == null) {
                    break;
                }
                ThreadState threadState = curPrefixProgress.slowState;
                if (threadState == null) {
                    break;
                }
                threadState.fastForward(state, table, splitKey, joinCtr);
                if (state.roundCtr < 0) {
                    break;
                }
            }
        }

        return state;
    }

    public State continueFromSP(JoinOrder joinOrder, int threadID) {
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        ThreadProgress curPrefixProgress = sharedProgress;
        int upperLevel = Math.min(THRESHOLD, nrTables);

        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null || curPrefixProgress.latestStates[threadID] == null) {
                return state;
            }
            // get a thread state
            ThreadState threadState = curPrefixProgress.latestStates[threadID];
            // if the state contains the progress for a given key
            if (threadState.hasProgress(0)) {
                threadState.fastForward(state, table, 0, joinCtr);
                if (state.roundCtr < 0) {
                    return state;
                }
            }
            else {
                return state;
//                if (previousSplitKey >= 0) {
//                    int slowestProgress;
//                    // TODO: slowest threads.
//                    if (slowThreads != null) {
//                        int slowID = 0;
//                        slowestProgress = curPrefixProgress.getSlowestProgress(previousSplitKey, slowID);
//                    }
//                    else {
//                        slowestProgress = curPrefixProgress.getSlowestProgress(previousSplitKey);
//                    }
//                    if (slowestProgress >= 0) {
//                        state.tupleIndices[table] = slowestProgress;
//                    }
//                    return state;
//                }
//                else {
//                    return state;
//                }
            }
        }

        if (THRESHOLD >= nrTables) {
            return state;
        }

        int intermediateTable = joinOrder.order[THRESHOLD];
        if (curPrefixProgress.childNodes[intermediateTable] != null &&
                curPrefixProgress.childNodes[intermediateTable].slowState.threadID == threadID) {
            for (int joinCtr = THRESHOLD; joinCtr < nrTables; joinCtr++) {
                int table = order[joinCtr];
                curPrefixProgress = curPrefixProgress.childNodes[table];
                if (curPrefixProgress == null) {
                    break;
                }
                ThreadState threadState = curPrefixProgress.slowState;
                if (threadState == null) {
                    break;
                }
                threadState.fastForward(state, table, 0, joinCtr);
                if (state.roundCtr < 0) {
                    break;
                }
            }
        }

        return state;
    }

    public int[] getSpace() {
        int lower = getLowerSpace();
        int upper = getUpperSpace();
        return new int[]{lower, upper};
    }

    public int getLowerSpace() {

        return getChildLowerProgressSpace(sharedProgress);
    }

    public int getUpperSpace() {

        return getChildUpperProgressSpace(sharedProgress);
    }


    public int getChildLowerProgressSpace(ThreadProgress progress) {
        int n = progress.slowState == null ? 0 : 1;
        for (ThreadProgress childProgress : progress.childNodes) {
            if (childProgress != null) {
                n += getChildLowerProgressSpace(childProgress);
            }
        }
        return n;
    }

    public int getChildUpperProgressSpace(ThreadProgress progress) {
        int n = progress.slowState == null ? 1 : 0;
        for (ThreadProgress childProgress : progress.childNodes) {
            if (childProgress != null) {
                n += getChildUpperProgressSpace(childProgress);
            }
        }
        return n;
    }
}
