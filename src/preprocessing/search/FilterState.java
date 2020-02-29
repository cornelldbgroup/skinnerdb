package preprocessing.search;

import expressions.compilation.UnaryBoolEval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FilterState {
    public final int[] order;
    public boolean avoidBranching;
    public List<Integer> actions;

    public int indexedTil;

    public UnaryBoolEval cachedEval;
    public int cachedTil;

    public int batches;
    public int batchSize;

    public FilterState(int numPredicates) {
        this.order = new int[numPredicates];
        this.avoidBranching = false;
        this.cachedEval = null;
        this.cachedTil = -1;
        this.batches = 1;
        this.indexedTil = -1;
        actions = new ArrayList<>();
    }

    @Override
    public String toString() {
        return "FilterState{" +
                "order=" + Arrays.toString(order) +
                ", avoidBranching=" + avoidBranching +
                ", cachedTil=" + cachedTil +
                ", batches=" + batches +
                '}';
    }
}
