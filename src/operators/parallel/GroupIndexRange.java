package operators.parallel;

import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import operators.Group;

import java.util.HashMap;
import java.util.Map;

public class GroupIndexRange {
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
    public Map<Group, Integer> valuesMap;
    /**
     *  Pre-compute the prefix for each data in the range.
     */
    public IntIntMap prefixMap;
    /**
     * A list of groups;
     */
    public Group[] groups;
    /**
     * First tuple index in range.
     */
    public int firstValue;
    /**
     * Last tuple index in range.
     */
    public int lastValue;
    /**
     * Last tuple index in range.
     */
    public int lastID;
    /**
     * Add more id.
     */
    public int addID;

    /**
     * Initialize range for first and
     * last tuple index.
     *
     * @param firstTuple	index of first tuple in range
     * @param lastTuple		index of last tuple in range
     */
    public GroupIndexRange(int firstTuple, int lastTuple, int bid) {
        this.firstTuple = firstTuple;
        this.lastTuple = lastTuple;
        this.bid = bid;
        valuesMap = new HashMap<>(lastTuple - firstTuple);
        groups = new Group[lastTuple - firstTuple + 1];
    }

    public void add(Group datum, int index) {
        int nrValue = valuesMap.getOrDefault(datum, 0);
        valuesMap.put(datum, nrValue + 1);
        groups[index] = datum;
    }
}
