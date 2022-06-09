package joining.parallel.indexing;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

/**
 * Represents a range of index creation.
 *
 * @author Anonymous
 *
 */
public class ArrayRange {
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
     *  Pre-compute the prefix for each data in the range.
     */
    public int[] prefixSum;
    /**
     * Initialize range for first and
     * last tuple index.
     *
     * @param firstTuple	index of first tuple in range
     * @param lastTuple		index of last tuple in range
     */
    public ArrayRange(int firstTuple, int lastTuple, int bid, int maxValue) {
        this.firstTuple = firstTuple;
        this.lastTuple = lastTuple;
        this.bid = bid;
        this.prefixSum = new int[maxValue + 1];
    }

    public void add(int datum) {
        this.prefixSum[datum] += 1;
    }

}
