package preprocessing.search;

public class FilterSearchConfig {
    public static final int ROWS_PER_TIMESTEP = 5000;
    public static final int LEAF_ROWS_PER_TIMESTEP = 15000;
    public static final double EXPLORATION_FACTOR = 1e-5;
    public static final int ROW_PARALLEL_ACTIONS = 4;
    public static final int ROW_PARALLEL_DELTA = 5;
    public static final boolean ENABLE_ROW_PARALLELISM = true;
    public static final boolean ENABLE_COMPILATION = true;
    public static final boolean FORGET = false;
    public static final int MAX_SIMULATIONS = 10;
}
