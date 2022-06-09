package joining.parallel.progress;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.GeneralConfig;
import config.JoinConfig;
import joining.parallel.join.DPJoin;
import joining.plan.JoinOrder;
import joining.progress.State;

import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Keeps track of progress made in evaluating different
 * join orders for threads. Offers methods to store progress made
 * and methods to retrieve the state from which evaluation
 * for one specific join order should continue.
 *
 * @author Anonymous
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
    public int[][][] tableOffsetMaps;
    /**
     * The slowest threads.
     */
    private int[] slowestThreads;
    /**
     * upper level thread
     */
    private static int THRESHOLD = Integer.MAX_VALUE;
    /**
     * Read write lock for left most table.
     */
    private ReadWriteLock[] locks;
    private int[] synchronizedTime;
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
        for (int j = 0; j < nrThreads; j++) {
            previousSplitTable[j] = HashIntIntMaps.newMutableMap();
        }

        synchronizedTime = new int[nrTables];
        locks = new ReadWriteLock[nrTables];
        for (int i = 0; i < nrTables; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
    }

    public void resetTracker() {
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
            sharedProgress.childNodes[joinCtr] = null;
        }
        Arrays.fill(tableOffsetMaps[0][0], 0);
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
        boolean[] checkThreads = new boolean[nrThreads];
        Arrays.fill(checkThreads, true);
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

//            if (checking) {
//                int slowestFlag = curPrefixProgress.isSlowest(splitKey, threadID, stateIndex, checkThreads);
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
        boolean isFast = false;
        int leftTable = joinOrder.order[0];
        roundCtr = requireWriteLock(leftTable, roundCtr);
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads,false);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];

            if (JoinConfig.PROGRESS_SHARING) {
                ThreadState tableState = curPrefixProgress.latestStates[0];
                if (tableState == null) {
                    state.roundCtr = roundCtr;
                    curPrefixProgress.latestStates[0] = new ThreadState(stateIndex, 0, roundCtr,
                            state.lastIndex, 0, 1);
                }
                else {
                    if (isFast) {
                        int[][] epochData = tableState.tableTupleIndexEpoch;
                        epochData[0][0] = stateIndex;
                        epochData[0][1] = roundCtr;
                        epochData[0][2] = state.lastIndex;
                        state.roundCtr = roundCtr;
                    }
                    else {
                        int fast = tableState.updateProgress(state, stateIndex, roundCtr);
                        if (fast < 0) {
                            break;
                        }
                        else if (fast > 0) {
                            isFast = true;
                        }
                    }
                }
            }
            else {
                ThreadState tableState = curPrefixProgress.latestStates[threadID];
                if (tableState == null) {
                    state.roundCtr = roundCtr;
                    curPrefixProgress.latestStates[threadID] = new ThreadState(stateIndex, 0, roundCtr,
                            state.lastIndex, threadID, 1);
                }
                else {
                    tableState.updateProgress(state, stateIndex, 0, roundCtr, state.lastIndex);
                }
            }
        }
        releaseWriteLock(leftTable);
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
        tableOffsetMaps[threadID][0][firstTable] = Math.max(lastTreatedTuple, tableOffsetMaps[threadID][0][firstTable]);
    }

    /**
     * Returns state from which evaluation of the given join order
     * must start in order to guarantee that all results are generated.
     *
     * @param joinOrder  a join order
     * @return start state for evaluating join order
     */
    public State continueFrom(JoinOrder joinOrder, int splitKey,
                              int threadID, boolean slowThreads, DPJoin dpJoin) {
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        ThreadProgress curPrefixProgress = sharedProgress;
        int upperLevel = Math.min(THRESHOLD, nrTables);

        int key = joinOrder.getPrefixKey(Math.min(2, nrTables));
        int previousSplitKey = previousSplitTable[threadID].getOrDefault(key, -1);
        boolean otherSplitKey = previousSplitKey != splitKey;
        boolean slowSet = false;
        int slowestID = -1;
        state.lastIndex = nrTables;
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null || curPrefixProgress.latestStates[threadID] == null) {
                state.lastIndex = joinCtr;
                return state;
            }
            // get a thread state
            ThreadState threadState = curPrefixProgress.latestStates[threadID];
            // if the state contains the progress for a given key
            boolean hasProgress = threadState.hasProgress(splitKey);
            if (hasProgress && !otherSplitKey) {
                threadState.fastForward(state, table, splitKey, joinCtr);
                if (state.roundCtr < 0) {
                    state.lastIndex = joinCtr;
                    return state;
                }
            }
            else {
                state.lastIndex = Math.min(state.lastIndex, joinCtr);
                if (previousSplitKey >= 0) {
                    if (slowestID >= 0 && curPrefixProgress.latestStates[slowestID] != null) {
                        int slowestProgress = curPrefixProgress.latestStates[slowestID].getProgress(previousSplitKey);
//                        dpJoin.writeLog("SlowID: " + slowestID + "\tProgress: " + slowestProgress);
                        if (slowestProgress >= 0) {
                            state.tupleIndices[table] = slowestProgress;
                        }
                    }
                    else {
                        int slowestProgress = curPrefixProgress.getSlowestProgress(previousSplitKey);
                        if (hasProgress && !slowSet) {
                            int progress = threadState.getProgress(splitKey);
                            if (progress > slowestProgress) {
                                otherSplitKey = false;
                                threadState.fastForward(state, table, splitKey, joinCtr);
                                if (state.roundCtr < 0) {
                                    state.lastIndex = joinCtr;
                                    return state;
                                }
                            }
                            else if (progress == slowestProgress) {
                                threadState.fastForward(state, table, splitKey, joinCtr);
                                if (state.roundCtr < 0) {
                                    state.lastIndex = joinCtr;
                                    return state;
                                }
                            }
                            else {
                                state.tupleIndices[table] = Math.max(0, slowestProgress);
                                slowSet = true;
                                state.lastIndex = joinCtr;
                                return state;
                            }
                        }
                        else {
                            state.tupleIndices[table] = Math.max(0, slowestProgress);
                            state.lastIndex = joinCtr;
                            return state;
                        }
                    }
                }
                else {
                    state.lastIndex = joinCtr;
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

    public State continueFromSP(JoinOrder joinOrder, int threadID, int firstTable) {
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        ThreadProgress curPrefixProgress = sharedProgress;
        int upperLevel = Math.min(THRESHOLD, nrTables);
        int leftTable = joinOrder.order[0];
        requireReadLock(leftTable);
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (JoinConfig.PROGRESS_SHARING) {
                if (curPrefixProgress == null || curPrefixProgress.latestStates[0] == null) {
                    releaseReadLock(leftTable);
                    return state;
                }
                // get a thread state
                ThreadState threadState = curPrefixProgress.latestStates[0];
                // if the state contains the progress for a given key
                if (threadState.hasProgress(0)) {
                    threadState.fastForward(state, table, 0, joinCtr);
                    if (state.roundCtr < 0) {
                        releaseReadLock(leftTable);
                        return state;
                    }
                }
                else {
                    releaseReadLock(leftTable);
                    return state;
                }
            }
            else {
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
                }
            }
        }
        releaseReadLock(leftTable);
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

    public void updateProgressSP(JoinOrder joinOrder,
                                 State state, int roundCtr, int firstTable) {
        // Update state for all join order prefixes
        ThreadProgress curPrefixProgress = sharedProgress;
        // Iterate over position in join order
        int upperLevel = Math.min(THRESHOLD, nrTables);
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads,false);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            ThreadState tableState = curPrefixProgress.latestStates[0];
            if (tableState == null) {
                state.roundCtr = roundCtr;
                curPrefixProgress.latestStates[0] = new ThreadState(stateIndex, 0, roundCtr,
                        state.lastIndex, 0, 1);
            }
            else {
                tableState.updateProgress(state, stateIndex, 0, roundCtr, state.lastIndex);
            }
        }
    }

    public void updateAheadProgress(JoinOrder joinOrder,
                                 State state, State otherState, int roundCtr) {
        // Update state for all join order prefixes
        ThreadProgress curPrefixProgress = sharedProgress;
        // Iterate over position in join order
        int upperLevel = Math.min(THRESHOLD, nrTables);
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, nrThreads,false);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            ThreadState tableState = curPrefixProgress.latestStates[0];
            if (tableState == null) {
                state.roundCtr = roundCtr;
                curPrefixProgress.latestStates[0] = new ThreadState(stateIndex, 0, roundCtr,
                        state.lastIndex, 0, 1);
            }
            else {
                tableState.updateProgress(state, stateIndex, 0, roundCtr, state.lastIndex);
            }
        }
    }

    public State continueFromSP(JoinOrder joinOrder) {
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        ThreadProgress curPrefixProgress = sharedProgress;
        int upperLevel = Math.min(THRESHOLD, nrTables);
        for (int joinCtr = 0; joinCtr < upperLevel; ++joinCtr) {
            int table = order[joinCtr];
            curPrefixProgress = curPrefixProgress.childNodes[table];
            if (curPrefixProgress == null || curPrefixProgress.latestStates[0] == null) {
                return state;
            }
            // get a thread state
            ThreadState threadState = curPrefixProgress.latestStates[0];
            // if the state contains the progress for a given key
            if (threadState.hasProgress(0)) {
                threadState.fastForward(state, table, 0, joinCtr);
                if (state.roundCtr < 0) {
                    return state;
                }
            }
            else {
                return state;
            }
        }
        return state;
    }

    public long getSize() {
        return getChildProgressSize(sharedProgress);
    }

    public long getChildProgressSize(ThreadProgress progress) {
        long n = 0;
        for (ThreadState state: progress.latestStates) {
            if (state != null) {
                n += (state.tableTupleIndexEpoch.length * 3 * 4);
//                n += (state.tableTupleIndexEpoch.length * (nrTables + 1) * 4);
            }
        }
        for (ThreadProgress childProgress : progress.childNodes) {
            if (childProgress != null) {
                n += getChildProgressSize(childProgress);
            }
        }
        return n;
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

    public void requireReadLock(int leftTable) {
        if (JoinConfig.PROGRESS_SHARING) {
            locks[leftTable].readLock().lock();
        }
    }

    public void releaseReadLock(int leftTable) {
        if (JoinConfig.PROGRESS_SHARING) {
            locks[leftTable].readLock().unlock();
        }
    }

    public int requireWriteLock(int leftTable, int roundCtr) {
        if (JoinConfig.PROGRESS_SHARING) {
            locks[leftTable].writeLock().lock();
            synchronizedTime[leftTable]++;
            roundCtr = synchronizedTime[leftTable];
        }
        return roundCtr;
    }

    public void releaseWriteLock(int leftTable) {
        if (JoinConfig.PROGRESS_SHARING) {
            locks[leftTable].writeLock().unlock();
        }
    }
}
