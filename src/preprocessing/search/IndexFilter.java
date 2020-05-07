package preprocessing.search;

import indexing.HashIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.Arrays;
import java.util.List;

public class IndexFilter {
    private List<HashIndex> indices;
    private List<List<Integer>> dataLocations;

    public IndexFilter(List<HashIndex> indices,
                       List<List<Integer>> dataLocations) {
        this.indices = indices;
        this.dataLocations = dataLocations;
    }

    public Pair<MutableIntSet, Boolean> getCandidateRowsFromIndex(
            FilterState state, int start, int LAST_ROW) {
        int end = Math.min(start + state.batchSize * state.batches,
                LAST_ROW);

        MutableIntSet candidate = IntSets.mutable.empty();
        for (int i = 0; i <= state.indexedTil; i++) {
            int pred = state.order[i];
            List<Integer> dataLocs = dataLocations.get(pred);
            HashIndex index = indices.get(pred);

            if (dataLocs.size() > 1) {
                // Union over all or predicates
                MutableIntSet predicateRows = IntSets.mutable.empty();
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
                    predicateRows.addAll(Arrays.copyOfRange(index.data,
                            startIdx, endIdx + 1));
                }
                if (i == 0) {
                    candidate = predicateRows;
                } else {
                    candidate.retainAll(predicateRows);
                }
            } else {
                // Fast handling for common case of 1 equality predicate
                int dataLoc = dataLocs.get(0);
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


        }

        return Pair.of(candidate, false);
    }
}
