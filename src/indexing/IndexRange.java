package indexing;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;


import java.util.HashMap;
import java.util.Map;

/**
 * Represents a range of index creation.
 *
 * @author Ziyun Wei
 *
 */
public class IndexRange {
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
     *
     */
    public IntIntMap valuesMap;
    public int[] offsets;
    /**
     * Initialize range for first and
     * last tuple index.
     *
     * @param firstTuple	index of first tuple in range
     * @param lastTuple		index of last tuple in range
     */
    public IndexRange(int firstTuple, int lastTuple, int bid) {
        this.firstTuple = firstTuple;
        this.lastTuple = lastTuple;
        this.bid = bid;
        valuesMap = HashIntIntMaps.newMutableMap();
    }

    public void add(int datum) {
        int nrValue = valuesMap.getOrDefault(datum, 0);
        valuesMap.put(datum, nrValue + 1);
    }

}
