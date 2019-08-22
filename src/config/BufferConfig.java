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
    public static final int maxRows = 100000;
    /**
     * Choose the size of a data page loaded for a column.
     */
    public static final int pageSize = 4000;
    /**
     * Whether to load data by pages.
     */
    public static boolean loadPage = true;
}
