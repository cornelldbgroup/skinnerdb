package joining.uct;

import java.util.ArrayList;
import java.util.List;

public class BaseUctLeaf extends BaseUctNode {
    /**
     * Initialize concurrent UCT root node.
     *
     * @param parent
     * @param label
     */
    public BaseUctLeaf(BaseUctNode parent, int label) {
        super(parent, label);
    }

    @Override
    public double sample() {
        return 0;
    }

    @Override
    public BaseUctNode maxRewardChild() {
        return null;
    }
}
