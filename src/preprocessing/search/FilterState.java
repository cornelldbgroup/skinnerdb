package preprocessing.search;

import expressions.compilation.UnaryBoolEval;

import java.util.Arrays;

public class FilterState {
    public final int[] order;
    public boolean useIndexScan;
    public boolean avoidBranching;

    public int indexedTil;

    public UnaryBoolEval cachedEval;
    public int cachedTil;

    public int parallelBatches;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
        this.useIndexScan = false;
        this.cachedEval = null;
        this.cachedTil = -1;
        this.parallelBatches = 0;
        this.indexedTil = -1;
    }

    @Override
    public String toString() {
        return "FilterState{" +
                "order=" + Arrays.toString(order) +
                ", useIndexScan=" + useIndexScan +
                ", avoidBranching=" + avoidBranching +
                ", cachedTil=" + cachedTil +
                ", parallelBatches=" + parallelBatches +
                '}';
    }

    public void reset() {
        this.avoidBranching = false;
        this.useIndexScan = false;
        this.cachedEval = null;
        this.cachedTil = -1;
        this.indexedTil = -1;
        this.parallelBatches = 0;
    }
}
