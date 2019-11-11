package joining.parallel.progress;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import joining.plan.JoinOrder;
import joining.progress.State;

public class ParallelProgressTracker {
    /**
     * Number of tables in the query.
     */
    final int nrTables;
    /**
     * Stores progress made for each join order prefix.
     */
    final ThreadProgress sharedProgress;
    /**
     * For each table the last tuple that was fully treated.
     */
    public final int[] tableOffset;
    /**
     * number of threads
     */
    final int nrThreads;
    /**
     * record previous split key for each join order for each thread
     */
    public IntIntMap[] previousSplitTable;
    /**
     * offsets for different split tables and different threads
     */
    public IntIntMap[][] tableOffsetMaps;
    /**
     * upper level thread
     */
    public static int THRESHOLD = Integer.MAX_VALUE;

    /**
     * Initializes pointers to child nodes.
     *
     * @param nrTables number of tables in the database
     */
    public ParallelProgressTracker(int nrTables, int nrThreads) {
        this.nrTables = nrTables;
        this.nrThreads = nrThreads;
        sharedProgress = new ThreadProgress(nrTables, nrThreads, false);
        tableOffset = new int[nrTables];
        previousSplitTable = new IntIntMap[nrThreads];
        tableOffsetMaps = new IntIntMap[nrThreads][nrTables];
        for (int j = 0; j < nrThreads; j++) {
            previousSplitTable[j] = HashIntIntMaps.newMutableMap();
            for (int i = 0; i < nrTables; i++) {
                tableOffsetMaps[j][i] = HashIntIntMaps.newMutableMap();
            }
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
    public void updateProgress(JoinOrder joinOrder, int splitKey,
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
                curPrefixProgress.latestStates[threadID] = new ThreadState(stateIndex, splitKey, roundCtr, state.lastIndex, threadID);
            }
            else {
                curPrefixProgress.latestStates[threadID].updateProgress(state, stateIndex, splitKey, roundCtr, state.lastIndex);
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
                        curPrefixProgress.slowState = new ThreadState(state.tupleIndices[table], splitKey, roundCtr, state.lastIndex, threadID);
                    }
                    else {
                        curPrefixProgress.slowState.updateProgress(state, stateIndex, splitKey, roundCtr, state.lastIndex);
                        curPrefixProgress.slowState.threadID = threadID;
                    }
                }
            }
            else if (lowerProgress == null || slowest) {
                ThreadProgress hybridProgressRoot = new ThreadProgress(nrThreads, nrThreads, true);
                hybridProgressRoot.slowState = new ThreadState(state.tupleIndices[intermediateTable], splitKey, roundCtr, state.lastIndex, threadID);
                ThreadProgress hybridProgress = hybridProgressRoot;

                for (int joinCtr = THRESHOLD + 1; joinCtr < nrTables; ++joinCtr) {
                    int table = joinOrder.order[joinCtr];
                    hybridProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads, true);
                    hybridProgress = hybridProgress.childNodes[table];
                    hybridProgress.slowState = new ThreadState(state.tupleIndices[table], splitKey, roundCtr, state.lastIndex, threadID);
                }
                curPrefixProgress.childNodes[intermediateTable] = hybridProgressRoot;
            }
        }
        // Join Order Prefix
        int key = joinOrder.getPrefixKey(2);
        // save previous split key
        previousSplitTable[threadID].putIfAbsent(key, splitKey);

        // Update table offset considering last fully treated tuple
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        tableOffsetMaps[threadID][firstTable].put(splitKey, lastTreatedTuple);
        int currentOffset = Integer.MAX_VALUE;
        for (int tid = 0; tid < nrThreads; tid++) {
            currentOffset = Math.min(tableOffsetMaps[tid][firstTable].getOrDefault(splitKey, 0), currentOffset);
        }
        tableOffset[firstTable] = Math.max(currentOffset, tableOffset[firstTable]);
    }

    /**
     * Returns state from which evaluation of the given join order
     * must start in order to guarantee that all results are generated.
     *
     * @param joinOrder  a join order
     * @return start state for evaluating join order
     */
    public State continueFrom(JoinOrder joinOrder, int splitKey,
                              int threadID, int splitTable, int[] slowThreads) {
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        ThreadProgress curPrefixProgress = sharedProgress;
        int key = joinOrder.getPrefixKey(2);
        int previousSplitKey = previousSplitTable[threadID].getOrDefault(key, -1);
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
            if (threadState.hasProgress(splitKey)) {
                threadState.fastForward(state, table, splitKey, joinCtr);
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
                threadState.fastForward(state, table, splitKey, joinCtr);
                if (state.roundCtr < 0) {
                    break;
                }
            }
        }

        return state;
    }

    public int[] getTableOffset() {
        int[] offset = new int[nrTables];
        for (int j = 0; j < nrTables; j++) {
            int currentOffset = Integer.MAX_VALUE;
            currentOffset = Math.min(tableOffset[j], currentOffset);
            offset[j] = currentOffset;
        }
        return offset;
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
