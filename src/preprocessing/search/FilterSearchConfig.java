package preprocessing.search;

public class FilterSearchConfig {
    public static final int ROWS_PER_TIMESTEP = 1000;
    public static final int PARALLEL_ROWS_PER_TIMESTEP = 10000;
    public static final double EXPLORATION_FACTOR = 1e-5;
    public static final int ROW_PARALLEL_ACTIONS = 4;
    public static final int ROW_PARALLEL_DELTA = 10;
    public static final boolean ENABLE_ROW_PARALLELISM = true;
    public static final boolean ENABLE_COMPILATION = true;
    public static final boolean FORGET = true;
}
