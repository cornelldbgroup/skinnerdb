package joining.join;

/**
 * In order to accelerate the index access,
 * access information is cached for each thread.
 *
 * @author Ziyun Wei
 */
public class IndexAccessInfo {
    /**
     * Cache value of last index access.
     */
    public int lastValue = -1;
    /**
     * Cache position returned during last
     * index access.
     */
    public int lastPos = -1;
    /**
     * Cache tuple returned during
     * last index access.
     */
    public int lastTuple = -1;
    /**
     * Cache tuple returned during
     * last index access.
     */
    public int lastNrVals = -1;

    /**
     * Reset all cached information to -1
     */
    public void reset() {
        lastPos = -1;
        lastTuple = -1;
        lastNrVals = -1;
        lastValue = -1;
    }
}
