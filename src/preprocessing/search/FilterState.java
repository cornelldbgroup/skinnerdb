package preprocessing.search;

public class FilterState {
    public final int[] order;
    public boolean useIndexScan;
    public boolean avoidBranching;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
        this.useIndexScan = false;
    }
}
