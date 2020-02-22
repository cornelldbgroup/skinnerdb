package preprocessing.uct;

import expressions.compilation.UnaryBoolEval;
import uct.Action;

import java.util.Arrays;

public class FilterAction implements Action {
    public enum ActionType {
        BRANCHING, AVOID_BRANCHING, INDEX_SCAN
    }

    public final int[] order;
    public ActionType type;
    public UnaryBoolEval cachedEval;
    public int cachedTil;

    public FilterAction(int numPredicates) {
        this.order = new int[numPredicates];
        this.type = ActionType.BRANCHING;
        this.cachedEval = null;
        this.cachedTil = 0;
    }

    @Override
    public String toString() {
        return "FilterState{" +
                "order=" + Arrays.toString(order) +
                ", type=" + type.name() +
                '}';
    }

    public void reset() {
        this.type = ActionType.BRANCHING;
        this.cachedEval = null;
        this.cachedTil = -1;
    }
}
