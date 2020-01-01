package joining.parallel.indexing;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

/**
 * Represents a range of index creation.
 *
 * @author Ziyun Wei
 *
 */
public class IntIndexRange {
    /**
     * First tuple index in range.
     */
    public final int firstTuple;
    /**
     * Last tuple index in range.
     */
    public final int lastTuple;
    /**
     * ID of a batch.
     */
    public final int bid;
    /**
     * Count the occurrence of each data within the range.
     */
    public IntIntMap valuesMap;
    /**
     *  Pre-compute the prefix for each data in the range.
     */
    public IntIntMap prefixMap;
    /**
     * Initialize range for first and
     * last tuple index.
     *
     * @param firstTuple	index of first tuple in range
     * @param lastTuple		index of last tuple in range
     */
    public IntIndexRange(int firstTuple, int lastTuple, int bid) {
        this.firstTuple = firstTuple;
        this.lastTuple = lastTuple;
        this.bid = bid;
        valuesMap = HashIntIntMaps.newMutableMap(lastTuple - firstTuple);
    }

    public void add(int datum) {
        int nrValue = valuesMap.getOrDefault(datum, 0);
        valuesMap.put(datum, nrValue + 1);
    }

}
