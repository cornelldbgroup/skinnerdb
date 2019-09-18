package joining.parallelization;
/**
 * The plan information of the first terminated thread.
 *
 *
 */
public class EndPlan {
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

    private int[] joinOrder;
    private int splitTable;

    public EndPlan(int nrTables) {
        joinOrder = new int[nrTables];
        splitTable = -1;
    }
}
