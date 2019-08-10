package config;
/**
 * Configures loading data from the disk or memory.
 *
 * @author ziyun wei
 *
 */
public class BufferConfig {
    /**
     * Choose the max number of rows to load.
     */
    public static final int maxRows = 200000;
    /**
     * Whether to store pruned data into memory.
     */
    public static final boolean storeMemory = true;
}
