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
    public final ThreadState[] latestStates;
    /**
     * the slowest progress state that is only useful for pure progress
     */
    public ThreadState slowState;



    public ThreadProgress(int nrTables, int nrThreads, boolean slow) {
        childNodes = new ThreadProgress[nrTables];
        if (!slow) {
            latestStates = new ThreadState[nrThreads];
        }
        else {
            latestStates = null;
        }
    }

    public int isSlowest(int splitKey, int threadID, int stateIndex, boolean[] checkThreads) {
        int flag = 1;
        for (int i = 0; i < latestStates.length; i++) {
            if (i != threadID && checkThreads[i]) {
                if (latestStates[i] != null) {
                    int[] newIndex = latestStates[i].tableTupleIndexEpoch[splitKey];
                    if (newIndex[1] > 0) {
                        if (stateIndex > newIndex[0]) {
                            return -1;
                        }
                        else if (stateIndex == newIndex[0]) {
                            flag = 0;
                        }
                        else {
                            checkThreads[i] = false;
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

    public int getSlowID(int splitKey, int threadID, int stateIndex, boolean[] checkThreads) {
        int slowID = -1;
        int slowProgress = Integer.MAX_VALUE;
        for (int i = 0; i < latestStates.length; i++) {
            if (latestStates[i] != null) {
                int[] newIndex = latestStates[i].tableTupleIndexEpoch[splitKey];
                if (newIndex[1] > 0) {
                    int progress = newIndex[0];
                    if (progress < stateIndex) {
                        slowID = i;
                        slowProgress = progress;
                    }
                    else if (progress == stateIndex) {

                    }
                    else {

                    }
                }
                else {
                    slowID = i;
                    break;
                }
            }
            else {
                slowID = i;
                break;
            }
        }
        return slowID;
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
