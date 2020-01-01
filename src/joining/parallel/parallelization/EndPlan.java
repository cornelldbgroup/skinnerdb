package joining.parallel.parallelization;

import config.ParallelConfig;
import joining.parallel.uct.BaseUctInner;
import joining.parallel.uct.SimpleUctNode;

import java.util.Arrays;

/**
 * The plan information of the first terminated thread.
 *
 * @author Ziyun Wei
 */
public class EndPlan {
    /**
     * Finished join order.
     */
    private int[] joinOrder;
    /**
     * Finished split table.
     */
    private int splitTable;
    /**
     * finish flags for different tables
     */
    public final boolean[][] finishFlags;
    /**
     * UCT extension of split tables
     */
    public SimpleUctNode tableRoot;
    /**
     * the slowest thread id for each split table
     */
    public int[] slowThreads;

    public EndPlan(int nrThreads, int nrTables, int[] cardinalities) {
        joinOrder = new int[nrTables];
        finishFlags = new boolean[nrThreads][nrTables];
        splitTable = -1;
        this.slowThreads = new int[nrTables];
        Arrays.fill(slowThreads, -1);
    }

    public int[] getJoinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(int[] joinOrder) {
        System.arraycopy(joinOrder, 0, this.joinOrder, 0, joinOrder.length);
    }

    public int getSplitTable() {
        return splitTable;
    }

    public void setSplitTable(int splitTable) {
        this.splitTable = splitTable;
    }

    public boolean setFinished(int tid, int splitTable) {
        finishFlags[tid][splitTable] = true;
        int number = 0;
        int slowID = 0;
        int firstFinish = -1;
        for (int i = 0; i < finishFlags.length && number <= 1; i++) {
            if (!finishFlags[i][splitTable]) {
                number++;
                slowID = i;
            }
            else if (firstFinish < 0){
                firstFinish = i;
            }
        }

        if (number == 1 && tid == firstFinish) {
            slowThreads[splitTable] = slowID;
//            tableRoot.updateStatistics(splitTable, 0);
            int selectedAction = -1;
            for (int i = 0; i < tableRoot.nextTable.length; i++) {
                if (tableRoot.nextTable[i] == splitTable) {
                    selectedAction = i;
                    break;
                }
            }
            tableRoot.accumulatedReward[selectedAction] = 0;
//            Arrays.fill(tableRoot.accumulatedReward, 0);
//            Arrays.fill(tableRoot.nrTries, 0);
//            tableRoot.nrVisits = 0;
        }

        if (number == 1) {
            tableRoot.updateStatistics(splitTable, 0);
        }

        return number == 0;
    }
}
