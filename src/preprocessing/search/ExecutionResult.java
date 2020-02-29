package preprocessing.search;

public class ExecutionResult {
    public final FilterUCTNode selected;
    public final double reward;
    public final FilterState state;
    public final int rows;

    public ExecutionResult(FilterUCTNode child, double reward,
                           FilterState state, int rows) {
        this.selected = child;
        this.reward = reward;
        this.state = state;
        this.rows = rows;
    }
}
