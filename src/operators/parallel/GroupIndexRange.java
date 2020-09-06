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
     * Array of group objects.
     */
    public Group[] groups;

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
        this.valuesMap = new HashMap<>(lastTuple - firstTuple + 1);
        this.groups = new Group[lastTuple - firstTuple + 1];
    }

    public void add(Group group, int index) {
        int size = valuesMap.size();
        valuesMap.putIfAbsent(group, size);
        groups[index] = group;
    }
}
