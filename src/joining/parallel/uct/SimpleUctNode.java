package joining.parallel.uct;

import config.JoinConfig;
import config.ParallelConfig;

import java.util.ArrayList;
import java.util.List;

public class SimpleUctNode {
    /**
     * Number of times this node was visited.
     */
    public int nrVisits = 0;
    /**
     * Number of times each action was tried out.
     */
    public final int[] nrTries;
    /**
     * Reward accumulated for specific actions.
     */
    public final double[] accumulatedReward;
    /**
     * Number of possible actions from this state.
     */
    public final int nrActions;
    /**
     * Associates each action index with a next table to split.
     */
    public final int[] nextTable;

    public SimpleUctNode(int[] joinOrder, int[] cardinalities) {
        List<Integer> splitTables = new ArrayList<>(joinOrder.length);
        int end = Math.min(5, joinOrder.length);
        for (int i = 0; i < end; i++) {
            int table = joinOrder[i];
            if (cardinalities[table] >= ParallelConfig.PARTITION_SIZE) {
                splitTables.add(table);
            }
        }
        nrActions = splitTables.size();
        nrTries = new int[nrActions];
        accumulatedReward = new double[nrActions];
        nextTable = new int[nrActions];
        for (int i = 0; i < splitTables.size(); i++) {
            nextTable[i] = splitTables.get(i);
        }
    }

    /**
     * Updates UCT statistics after sampling.
     *
     * @param table          table taken
     * @param reward         reward achieved
     */
    public void updateStatistics(int table, double reward) {
        int selectedAction = -1;
        for (int i = 0; i < nextTable.length; i++) {
            if (nextTable[i] == table) {
                selectedAction = i;
                break;
            }
        }
        ++nrVisits;
        ++nrTries[selectedAction];
        accumulatedReward[selectedAction] += reward;
    }

    public int getMaxOrderedUCTTable() {
        double maxUCT = -1;
        int maxUCTchild = nextTable[0];
        // Iterate over child nodes in specified order
        for (int i = 0; i < nrActions; i++) {
            int table = nextTable[i];
            int childVisits = nrTries[i];
            if (childVisits == 0) {
                return table;
            }
            else {
                double childReward = accumulatedReward[i] / childVisits;
                int nrVisits = this.nrVisits;
                double uctValue = childReward + 10E-20 * Math.sqrt(Math.log(nrVisits) / childVisits);
//                    double uctValue = childReward;
                if (uctValue > maxUCT) {
                    maxUCT = uctValue;
                    maxUCTchild = table;
                }
            }
        }
        return maxUCTchild;
    }
}
