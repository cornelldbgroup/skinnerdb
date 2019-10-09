package statistics;
/**
 * Statistics about the pre-processing phase - most statistics
 * refer to counts achieved during the last query
 * evaluation.
 *
 * @author immanueltrummer
 *
 */
public class PreStats {
    /**
     * Period of filtering columns.
     */
    public static long filterTime = 0;
    /**
     * Period of creating indices.
     */
    public static long indexTime = 0;
}
