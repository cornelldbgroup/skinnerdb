package joining.parallel.progress;

import joining.plan.JoinOrder;
import joining.progress.State;


public class ThreadProgressTracker {
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
     * Number of ways to split a table.
     */
    private final int nrSplits;
    /**
     * offsets for different split tables and different threads
     */
    public int[][] tableOffsetMaps;
    /**
     * Initializes pointers to child nodes.
     *
     * @param nrTables number of tables in the database
     */
    public ThreadProgressTracker(int nrTables, int nrSplits) {
        this.nrTables = nrTables;
        this.nrSplits = nrSplits;
        sharedProgress = new ThreadProgress(nrTables, 1, false);
        tableOffset = new int[nrTables];
        tableOffsetMaps = new int[nrSplits][nrTables];
    }

    public void resetTracker() {
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
            sharedProgress.childNodes[joinCtr] = null;
        }
    }

    /**
     * Integrates final state achieved when evaluating one specific
     * join order.
     *
     * @param joinOrder a join order evaluated with a specific time budget
     * @param state     final state achieved when evaluating join order
     */
    public boolean updateProgress(JoinOrder joinOrder, int splitKey,
                                  State state, int roundCtr, int firstTable) {
        // Update state for all join order prefixes
        ThreadProgress curPrefixProgress = sharedProgress;
        // Local id
        int threadID = 0;
        // Initialize the time version of the state.
        int tv = 0;
        // Whether we need to check the slowest progress for the upper level.
        boolean checking = true;
        // Iterate over position in join order
        boolean slowest = !state.isFinished();
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
            int table = joinOrder.order[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, 1, false);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            ThreadState tableState = curPrefixProgress.latestStates[threadID];
            if (tableState == null) {
                state.roundCtr = roundCtr;
                curPrefixProgress.latestStates[threadID] = new ThreadState(stateIndex, splitKey, roundCtr,
                        state.lastIndex, threadID, nrSplits);
            } else {
                curPrefixProgress.latestStates[threadID].updateProgress(state, stateIndex,
                        splitKey, roundCtr, state.lastIndex);
            }
        }

        // Update table offset considering last fully treated tuple
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        tableOffsetMaps[splitKey][firstTable] = lastTreatedTuple;
        tableOffset[firstTable] = Math.max(lastTreatedTuple, tableOffset[firstTable]);
        return slowest;
    }

    /**
     * Integrates final state achieved when evaluating one specific
     * join order.
     *
     * @param joinOrder a join order evaluated with a specific time budget
     * @param state     final state achieved when evaluating join order
     */
    public boolean fastAndUpdate(int[] joinOrder, State state, int roundCtr, int firstTable) {
        // Update state for all join order prefixes
        ThreadProgress curPrefixProgress = sharedProgress;
        // Local id
        int threadID = 0;
        // Initialize the time version of the state.
        int tv = 0;
        // Whether we need to check the state is ahead of saved progress.
        boolean isAhead = true;
        if (state == null) {
            return false;
        }
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
            int table = joinOrder[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                break;
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            ThreadState tableState = curPrefixProgress.latestStates[threadID];
            if (tableState == null) {
                break;
            } else {
                int progress = curPrefixProgress.latestStates[threadID].getProgress(0);
                if (progress > stateIndex) {
                    isAhead = false;
                    break;
                }
                else if (progress < stateIndex) {
                    break;
                }
            }
        }
        if (!isAhead) {
            return false;
        }
        // Iterate over position in join order
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
            int table = joinOrder[joinCtr];
            int stateIndex = state.tupleIndices[table];
            if (curPrefixProgress.childNodes[table] == null) {
                curPrefixProgress.childNodes[table] = new ThreadProgress(nrTables, 1, false);
            }
            curPrefixProgress = curPrefixProgress.childNodes[table];
            ThreadState tableState = curPrefixProgress.latestStates[threadID];
            if (tableState == null) {
                state.roundCtr = roundCtr;
                curPrefixProgress.latestStates[threadID] = new ThreadState(stateIndex, 0, roundCtr,
                        state.lastIndex, threadID, nrSplits);
            } else {
                curPrefixProgress.latestStates[threadID].updateProgress(state, stateIndex,
                        0, roundCtr, state.lastIndex);
            }
        }

        // Update table offset considering last fully treated tuple
        int lastTreatedTuple = state.tupleIndices[firstTable] - 1;
        tableOffset[firstTable] = Math.max(lastTreatedTuple, tableOffset[firstTable]);
        return true;
    }

    /**
     * Returns state from which evaluation of the given join order
     * must start in order to guarantee that all results are generated.
     *
     * @param joinOrder  a join order
     * @return start state for evaluating join order
     */
    public State continueFrom(JoinOrder joinOrder, int splitKey) {
        int[] order = joinOrder.order;
        State state = new State(nrTables);
        // Integrate progress from join orders with same prefix
        ThreadProgress curPrefixProgress = sharedProgress;
        // Local id
        int threadID = 0;

        state.lastIndex = nrTables;
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
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
            // Case 1: Same prefix and same split table
            if (hasProgress) {
                threadState.fastForward(state, table, splitKey, joinCtr);
                if (state.roundCtr < 0) {
                    state.lastIndex = joinCtr;
                    return state;
                }
            }
            // Case 2: No pre-stored progress for specific split key
            else  {
                state.lastIndex = joinCtr;
                return state;
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
