package preprocessing.search;

public class FilterState {
    public final int[] order;
    public final boolean avoidBranching;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
    }

    public boolean useIndexScan() {
        return order[0] < 0;
    }
}
