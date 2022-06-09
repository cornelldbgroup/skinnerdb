package joining.plan;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import query.QueryInfo;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Represents an intermediate result
 * (a set of table included in the join order).
 *
 * @author Anonymous
 *
 */
public class HotSet {
    /**
     * Number of tables that are joined.
     */
    public final int nrJoinedTables;
    /**
     * Order in which tables are joined.
     */
    public final IntSet hotSet;
    /**
     * The hash code for the intermediate result.
     */
    public final int hashCode;
    /**
     * The number of tables to join
     */
    public final int nrJoined;

    /**
     * Initializes a intermediate result.
     *
     * @param joinedTables	a set of joined tables
     */
    public HotSet(Set<Integer> joinedTables, int len) {
        this.nrJoinedTables = joinedTables.size();
        this.hotSet = HashIntSets.newImmutableSet(joinedTables);
        this.nrJoined = len;
        int hash = 0;
        int card = 1;
        for (int table : hotSet) {
            hash += table * card;
            card *= len;
        }
        this.hashCode = hash;
    }

    /**
     * Two hot sets are equal if they contain the same set.
     */
    @Override
    public boolean equals(Object otherSet) {
        if (otherSet == this) {
            return true;
        }
        if (!(otherSet instanceof HotSet)) {
            return false;
        }
        HotSet other = (HotSet) otherSet;
        return other.nrJoinedTables == nrJoinedTables
                && other.hashCode == hashCode;
    }
    /**
     * Hash code is based on intermediate result.
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return Arrays.toString(hotSet.toIntArray());
    }


    public Pair<Integer, Integer> getConstraint(IntSet cycle,
                                                QueryInfo query,
                                                IntSet next,
                                                Map<Integer, Integer> priority) {
        // check redundant
        if (cycle.containsAll(hotSet)) {
            return null;
        }
        IntSet outSet = HashIntSets.newMutableSet();
        for (int i = 0; i < nrJoined; i++) {
            if (!hotSet.contains(i) && query.connected(hotSet, i)) {
                outSet.add(i);
                int value = priority.getOrDefault(i, 0);
                priority.put(i, value + 1);
            }
        }
        int leftTable = -1;
        int rightTable = -1;
        List<Integer> in = hotSet
                .stream().sorted(
                        Comparator.comparing(i -> -1 * priority.getOrDefault(i, 0)))
                .collect(Collectors.toList());
        for (Integer leftCandidate : in) {
            leftTable = leftCandidate;

            // left is not in the cycle
            if (!cycle.contains(leftTable)) {
                if (outSet.size() > 0) {
                    cycle.add(leftTable);
                    List<Integer> out = outSet
                            .stream().sorted(
                                    Comparator.comparing(i -> -1 * priority.getOrDefault(i, 0)))
                            .collect(Collectors.toList());
                    rightTable = out.get(0);
                    cycle.add(rightTable);
                    break;
                }
//            ImmutableSet<Integer> rightContains = Sets.intersection(outSet, cycle).immutableCopy();
//            if (rightContains.size() > 0) {
//                ImmutableSet<Integer> rightPrior = Sets.difference(rightContains, next).immutableCopy();
//                rightTable = rightPrior.size() > 0 ? rightPrior.iterator().next() : rightContains.iterator().next();
//            }
//            else {
//                ImmutableSet<Integer> rightPrior = Sets.difference(outSet, next).immutableCopy();
//                IntCursor outCursor = outSet.cursor();
//                outCursor.moveNext();
//                rightTable = rightPrior.size() > 0 ? rightPrior.iterator().next() : outCursor.elem();
//                cycle.add(rightTable);
//            }
            }
            // left is in the cycle
            else {
                ImmutableSet<Integer> right = Sets.difference(outSet, cycle).immutableCopy();

                int rightSize = right.size();

                if (rightSize > 0) {
                    List<Integer> out = right.stream().sorted(
                            Comparator.comparing(i -> -1 * priority.getOrDefault(i, 0)))
                            .collect(Collectors.toList());
                    rightTable = out.get(0);
//                ImmutableSet<Integer> rightPrior = Sets.difference(right, next).immutableCopy();
//                rightTable = rightPrior.size() > 0 ? rightPrior.iterator().next() : right.iterator().next();

                    cycle.add(rightTable);
                    break;
                }
            }
        }
        if (leftTable < 0 || rightTable < 0) {
            return null;
        }

        if (outSet.size() == 1) {
            IntCursor nextCursor = outSet.cursor();
            nextCursor.moveNext();
            IntSet newHotSet = HashIntSets.newMutableSet();
            newHotSet.addAll(hotSet);
            int nextTable = nextCursor.elem();
            newHotSet.add(nextTable);
            priority.put(nextTable, 0);
            cycle.add(nextTable);
            for (int i = 0; i < nrJoined; i++) {
                if (!newHotSet.contains(i) && query.connected(newHotSet, i)) {
                    int value = priority.getOrDefault(i, 0);
                    priority.put(i, value + 1);
                }
            }

        }
        int leftPriority = priority.getOrDefault(leftTable, 1);
        int rightPriority = priority.getOrDefault(rightTable, 1);
        priority.put(leftTable, leftPriority - 1);
        priority.put(rightTable, rightPriority - 1);
        return new ImmutablePair<>(leftTable, rightTable);
    }
}
