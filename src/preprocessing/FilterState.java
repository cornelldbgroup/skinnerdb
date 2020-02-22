package preprocessing;

import expressions.compilation.UnaryBoolEval;

import java.util.Arrays;

public class FilterState {
    public final int[] order;
    public boolean useIndexScan;
    public boolean avoidBranching;

    public UnaryBoolEval cachedEval;
    public int cachedTil;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
        this.useIndexScan = false;
        this.cachedEval = null;
        this.cachedTil = 0;
    }

    @Override
    public String toString() {
        return "FilterState{" +
                "order=" + Arrays.toString(order) +
                ", useIndexScan=" + useIndexScan +
                ", avoidBranching=" + avoidBranching +
                '}';
    }

    public void reset() {
        this.avoidBranching = false;
        this.useIndexScan = false;
        this.cachedEval = null;
        this.cachedTil = -1;
    }
}
