package joining.parallel.indexing;

import com.koloboke.collect.map.DoubleIntMap;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashDoubleIntMaps;
import com.koloboke.collect.map.hash.HashIntIntMaps;

/**
 * Represents a range of index creation.
 *
 * @author Ziyun Wei
 *
 */
public class DoubleIndexRange {
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
    public DoubleIntMap valuesMap;
    /**
     *  Pre-compute the prefix for each data in the range.
     */
    public DoubleIntMap prefixMap;
    /**
     * Initialize range for first and
     * last tuple index.
     *
     * @param firstTuple	index of first tuple in range
     * @param lastTuple		index of last tuple in range
     */
    public DoubleIndexRange(int firstTuple, int lastTuple, int bid) {
        this.firstTuple = firstTuple;
        this.lastTuple = lastTuple;
        this.bid = bid;
        valuesMap = HashDoubleIntMaps.newMutableMap(lastTuple - firstTuple);
    }

    public void add(double datum) {
        int nrValue = valuesMap.getOrDefault(datum, 0);
        valuesMap.put(datum, nrValue + 1);
    }

}
