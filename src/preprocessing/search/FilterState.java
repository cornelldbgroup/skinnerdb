package preprocessing.search;

import expressions.compilation.UnaryBoolEval;

import java.util.Arrays;

public class FilterState {
    public final int[] order;
    public boolean useIndexScan;
    public boolean avoidBranching;

    public UnaryBoolEval cachedEval;
    public int cachedTil;

    public int batches;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
        this.useIndexScan = false;
        this.cachedEval = null;
        this.cachedTil = -1;
        this.batches = 0;
    }

    @Override
    public String toString() {
        return "FilterState{" +
                "order=" + Arrays.toString(order) +
                ", useIndexScan=" + useIndexScan +
                ", avoidBranching=" + avoidBranching +
                ", cachedTil=" + cachedTil +
                ", batches=" + batches +
                '}';
    }

    public void reset() {
        this.avoidBranching = false;
        this.useIndexScan = false;
        this.cachedEval = null;
        this.cachedTil = -1;
        this.batches = 0;
    }
}
