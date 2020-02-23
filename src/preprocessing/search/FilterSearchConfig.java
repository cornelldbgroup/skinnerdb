package preprocessing.search;

import parallel.ParallelService;

public class FilterSearchConfig {
    public static final int ROWS_PER_TIMESTEP = 1000;
    public static final int PARALLEL_ROWS_PER_TIMESTEP =
            10000 * ParallelService.HIGH_POOL_THREADS;
    public static final double EXPLORATION_FACTOR = 1e-5;
    public static boolean FORGET = true;
}
