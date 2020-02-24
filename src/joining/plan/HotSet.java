package joining.plan;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.koloboke.collect.IntCursor;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import query.QueryInfo;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Represents an intermediate result
 * (a set of table included in the join order).
 *
 * @author Ziyun Wei
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

    /**
     * Get the constraint, given some cycle violations and hot set.
     *
     * @param cycle     cycle violation
     * @return          a selected constraint
     */
    public Pair<Integer, Integer> getConstraint(IntSet cycle) {
        IntSet outSet = HashIntSets.newMutableSet();
        for (int i = 0; i < nrJoined; i++) {
            if (!hotSet.contains(i)) {
                outSet.add(i);
            }
        }
        IntCursor hotCursor = hotSet.cursor();
        IntCursor outCursor = outSet.cursor();

        hotCursor.moveNext();
        outCursor.moveNext();

        ImmutableSet<Integer> left = Sets.difference(hotSet, cycle).immutableCopy();
        ImmutableSet<Integer> right = Sets.difference(outSet, cycle).immutableCopy();

        int leftSize = left.size();
        int rightSize = right.size();

        if (leftSize == 0 && rightSize == 0) {
            return null;
        }
        else if (leftSize == 0) {
            int leftTable = hotCursor.elem();
            int rightTable = right.iterator().next();
            cycle.add(rightTable);
            return new ImmutablePair<>(leftTable, rightTable);
        }
        else if (rightSize == 0) {
            int leftTable = left.iterator().next();
            int rightTable = outCursor.elem();
            cycle.add(leftTable);
            return new ImmutablePair<>(leftTable, rightTable);
        }
        else {
            if (cycle.size() > 0) {
                IntCursor cycleCursor = cycle.cursor();
                cycleCursor.moveNext();
                int selectedTable = cycleCursor.elem();
                if (hotSet.contains(selectedTable)) {
                    int rightTable = right.iterator().next();
                    cycle.add(rightTable);
                    return new ImmutablePair<>(selectedTable, rightTable);
                }
                else {
                    int leftTable = left.iterator().next();
                    cycle.add(leftTable);
                    return new ImmutablePair<>(leftTable, selectedTable);
                }
            }
            else {
                int leftTable = hotCursor.elem();
                int rightTable = outCursor.elem();
                cycle.add(leftTable);
                cycle.add(rightTable);
                return new ImmutablePair<>(leftTable, rightTable);
            }
        }
    }

    public Pair<Integer, Integer> getConstraint(IntSet cycle,
                                                Set<Pair<Integer, Integer>> constraints,
                                                Map<Integer, Integer> count,
                                                Map<Integer, Set<Integer>> joinConnection) {
        IntSet outSet = HashIntSets.newMutableSet();
        for (int i = 0; i < nrJoined; i++) {
            if (!hotSet.contains(i)) {
                outSet.add(i);
            }
        }
        List<Pair<Integer, Integer>> newConstraints = new ArrayList<>();
        constraints.forEach(pair -> {
            int left = pair.getLeft();
            int right = pair.getRight();
            if (((hotSet.contains(left) && outSet.contains(right)) ||
                    (hotSet.contains(right) && outSet.contains(left))) &&
                    !(cycle.contains(left) && cycle.contains(right))) {
                newConstraints.add(pair);
            }
        });

        if (newConstraints.size() > 0) {
            Comparator<Pair<Integer, Integer>> comparator = Comparator.comparing(e -> {
                int left = e.getLeft();
                int right = e.getRight();
                return count.getOrDefault(left, 0) + count.getOrDefault(right, 0);
            });
            newConstraints.sort(comparator);
            Pair<Integer, Integer> constraint = newConstraints.get(0);

            constraints.remove(constraint);



//            Pair<Integer, Integer> constraint = newConstraints.stream().filter(pair -> {
//                int left = pair.getLeft();
//                int right = pair.getRight();
//                return cycle.contains(left) || cycle.contains(right);
//            }).findFirst().orElse(null);
//
//            if (constraint != null) {
//                constraints.remove(constraint);
//            }
//            else {
//                constraint = newConstraints.iterator().next();
//                constraints.remove(constraint);
//            }

            int cLeft = constraint.getLeft();
            int cRight = constraint.getRight();

//            int leftCount = count.getOrDefault(cLeft, 0) + 1;
//            int rightCount = count.getOrDefault(cRight, 0) + 1;
//            if (joinConnection.get(cLeft).size() <= leftCount || joinConnection.get(cRight).size() <= rightCount) {
//                count.put(cLeft, Integer.MAX_VALUE);
//                count.put(cRight, Integer.MAX_VALUE);
//            }
//            else {
//                count.merge(cLeft, 1, Integer::sum);
//                count.merge(cRight, 1, Integer::sum);
//            }
            count.merge(cLeft, 1, Integer::sum);
            count.merge(cRight, 1, Integer::sum);
            cycle.add(cLeft);
            cycle.add(cRight);
            return constraint;
        }
        else {
            return null;
        }
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
