package preprocessing.search;

import indexing.HashIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.List;

public class IndexFilter {
    private List<HashIndex> indices;
    private List<List<Integer>> dataLocations;

    public IndexFilter(List<HashIndex> indices,
                       List<List<Integer>> dataLocations) {
        this.indices = indices;
        this.dataLocations = dataLocations;
    }

    public Pair<MutableIntList, Boolean> getCandidateRowsFromIndex(
            FilterState state, int start, int LAST_ROW) {
        int end = Math.min(start + state.batchSize * state.batches,
                LAST_ROW);

        MutableIntList candidate = null;
        for (int i = 0; i <= state.indexedTil; i++) {
            int pred = state.order[i];
            List<Integer> dataLocs = dataLocations.get(pred);
            HashIndex index = indices.get(pred);

            if (dataLocs.size() > 1) {
                // Union over all or predicates
                MutableIntSet merged = IntSets.mutable.empty();
                for (int dataLoc : dataLocs) {
                    int startIdx =
                            index.nextHighestRowInBucket(dataLoc, start - 1);
                    if (startIdx < 0) {
                        continue;
                    }
                    int endIdx =
                            index.nextSmallestRowInBucket(dataLoc,
                                    end);
                    if (endIdx < 0) endIdx = index.getBucketEnd(dataLoc);

                    for (int r = startIdx; r <= endIdx; r++) {
                        merged.add(index.data[r]);
                    }
                }
                if (i == 0) {
                    candidate = merged.toSortedList();
                } else {
                    candidate = intersect(candidate, merged);
                }
            } else {
                // Fast handling for common case of 1 equality predicate
                int dataLoc = dataLocs.get(0);
                int startIdx =
                        index.nextHighestRowInBucket(dataLoc, start - 1);
                if (startIdx < 0) {
                    return Pair.of(IntLists.mutable.empty(), true);
                }
                int endIdx =
                        index.nextSmallestRowInBucket(dataLoc,
                                end);
                if (endIdx < 0) endIdx = index.getBucketEnd(dataLoc);

                if (i == 0) {
                    candidate = IntLists.mutable.empty();
                    for (int r = startIdx; r <= endIdx; r++) {
                        candidate.add(index.data[r]);
                    }
                } else {
                    candidate = intersect(candidate, index.data, startIdx,
                            endIdx);
                }
            }


        }

        return Pair.of(candidate, false);
    }

    private MutableIntList intersect(MutableIntList l1, MutableIntSet l2) {
        MutableIntList res = IntLists.mutable.empty();
        for (int i = 0; i < l1.size(); i++) {
            int v = l1.get(i);
            if (l2.contains(v)) {
                res.add(v);
            }
        }
        return res;
    }

    private MutableIntList intersect(MutableIntList list, int[] arr,
                                     int startIdx, int endIdx) {
        MutableIntList res = IntLists.mutable.empty();
        int i1 = 0, i2 = startIdx;
        int n1 = list.size(), n2 = endIdx + 1;

        int v1, v2;
        while (i1 < n1 && i2 < n2) {
            v1 = list.get(i1);
            v2 = arr[i2];
            if (v1 == v2) {
                i1++;
                i2++;
                res.add(v1);
            } else if (v1 < v2) {
                i1++;
            } else {
                i2++;
            }
        }

        return res;
    }
}
