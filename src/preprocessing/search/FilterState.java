package preprocessing.search;

import java.util.Arrays;

public class FilterState {
    public final int[] order;
    public boolean useIndexScan;
    public boolean avoidBranching;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
        this.useIndexScan = false;
    }

    @Override
    public String toString() {
        return "FilterState{" +
                "order=" + Arrays.toString(order) +
                ", useIndexScan=" + useIndexScan +
                ", avoidBranching=" + avoidBranching +
                '}';
    }
}
