package joining.parallel.progress;

public class ThreadProgress {
    /**
     * Points to nodes describing progress for next table.
     */
    public final ThreadProgress[] childNodes;
    /**
     * Latest state reached by any join order sharing
     * a certain table prefix.
     */
    public ThreadState[] latestStates;
    /**
     * the slowest progress state that is only useful for pure progress
     */
    public ThreadState slowState;



    public ThreadProgress(int nrTables, int nrThreads, boolean slow) {
        childNodes = new ThreadProgress[nrTables];
        if (!slow) {
            latestStates = new ThreadState[nrThreads];
        }
    }

    public int isSlowest(int splitKey, int threadID, int stateIndex) {
        int flag = 1;
        for (int i = 0; i < latestStates.length; i++) {
            if (i != threadID) {
                if (latestStates[i] != null) {
                    int[] newIndex = latestStates[i].tableTupleIndexEpoch[splitKey];
                    if (newIndex[1] > 0) {
                        if (stateIndex > newIndex[0]) {
                            return -1;
                        }
                        else if (stateIndex == newIndex[0]) {
                            flag = 0;
                        }
                    }
                    else {
                        return -1;
                    }
                }
                else {
                    return -1;
                }
            }
        }
        return flag;
    }

    public int getSlowestProgress(int splitKey) {
        int slowestProgress = Integer.MAX_VALUE;
        for (ThreadState eachState : latestStates) {
            if (eachState != null) {
                int newIndex = eachState.getProgress(splitKey);
                if (newIndex < 0) {
                    return -1;
                }
                slowestProgress = Math.min(newIndex, slowestProgress);
            } else {
                return -1;
            }
        }
        return slowestProgress;
    }

    public int getSlowestProgress(int splitKey, int threadID) {
        int slowestProgress = Integer.MAX_VALUE;
        ThreadState eachState = latestStates[threadID];
        if (eachState != null && eachState.hasProgress(splitKey)) {
            int newIndex = eachState.tableTupleIndexEpoch[splitKey][0];
            slowestProgress = Math.min(newIndex, slowestProgress);
        }
        else {
            return -1;
        }
        return slowestProgress;
    }
}
