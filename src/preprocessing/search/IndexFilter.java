package preprocessing.search;

import indexing.HashIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.Arrays;
import java.util.List;

public class IndexFilter {
    private List<HashIndex> indices;
    private List<Integer> dataLocations;

    public IndexFilter(List<HashIndex> indices, List<Integer> dataLocations) {
        this.indices = indices;
        this.dataLocations = dataLocations;
    }

    public int nextHighestRowInBucket(MutableIntList list, int target,
                                      int start) {
        int end = list.size() - 1;

        int nextHighest = -1;
        while (start <= end) {
            int mid = (start + end) / 2;

            if (list.get(mid) <= target) {
                start = mid + 1;
            } else {
                nextHighest = mid;
                end = mid - 1;
            }
        }

        return nextHighest;
    }

    public Pair<MutableIntSet, Boolean> getCandidateRowsFromIndex(
            FilterState state, int start, int LAST_ROW) {
        int end = Math.min(start + state.batchSize * state.batches,
                LAST_ROW);

        MutableIntSet candidate = IntSets.mutable.empty();
        for (int i = 0; i <= state.indexedTil; i++) {
            int pred = state.order[i];
            int dataLoc = dataLocations.get(pred);
            HashIndex index = indices.get(pred);
            int startIdx =
                    index.nextHighestRowInBucket(dataLoc, start - 1);
            if (startIdx < 0) {
                return Pair.of(IntSets.mutable.empty(), true);
            }
            int endIdx =
                    index.nextSmallestRowInBucket(dataLoc,
                            end);
            if (endIdx < 0) endIdx = index.getBucketEnd(dataLoc);

            if (i == 0) {
                candidate.addAll(Arrays.copyOfRange(index.data, startIdx,
                        endIdx + 1));
            } else {
                candidate.retainAll(Arrays.copyOfRange(index.data, startIdx,
                        endIdx + 1));
            }
        }

        return Pair.of(candidate, false);
    }
}
