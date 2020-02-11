package preprocessing.search;

public class FilterState {
    public final int[] order;
    public final boolean useIndexScan;
    public final boolean avoidBranching;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
        this.useIndexScan = false;
    }
}
