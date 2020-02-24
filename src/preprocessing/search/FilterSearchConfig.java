package preprocessing.search;

public class FilterSearchConfig {
    public static final int ROWS_PER_TIMESTEP = 1000;
    public static final int PARALLEL_ROWS_PER_TIMESTEP = 15000;
    public static final double EXPLORATION_FACTOR = 1e-7;
    public static final int BRANCHING_PARALLEL_ACTIONS = 4;
    public static boolean FORGET = true;
}
