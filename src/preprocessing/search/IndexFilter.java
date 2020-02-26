package preprocessing.search;

import indexing.HashIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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

            if (list.get(mid) < target) {
                start = mid + 1;
            } else {
                nextHighest = mid;
                end = mid - 1;
            }
        }

        return nextHighest;
    }

    public Pair<MutableIntList, Integer> getRows(int budget, FilterState state,
                                                 int lastCompletedRow,
                                                 int LAST_TABLE_ROW) {
        int maxRow = 0;

        // compute union
        List<MutableIntList> predicateRows = new ArrayList<>();
        for (int i = 0; i <= state.indexedTil; i++) {
            MutableIntList rows = IntLists.mutable.empty();

            int pred = state.order[i];
            int dataLoc = dataLocations.get(pred);
            HashIndex index = indices.get(pred);
            int startIdx =
                    index.nextHighestRowInBucket(dataLoc, lastCompletedRow);
            if (startIdx < 0) {
                return Pair.of(IntLists.mutable.empty(), LAST_TABLE_ROW);
            }
            int endIdx =
                    index.nextHighestRowInBucket(dataLoc,
                            lastCompletedRow + budget - 1);
            if (endIdx < 0) endIdx = index.getBucketEnd(dataLoc);

            rows.addAll(Arrays.copyOfRange(index.data, startIdx, endIdx + 1));
            predicateRows.add(rows);
        }
        predicateRows.sort(Comparator.comparingInt(l -> l.size()));


        MutableIntList base = predicateRows.get(0);
        maxRow = base.get(base.size() - 1);

        if (predicateRows.size() == 1) return Pair.of(base, maxRow);

        MutableIntList result = IntLists.mutable.empty();

        int i = 0;
        ROW_LOOP:
        while (i < base.size()) {
            int row = base.get(i);

            int max = -1;
            for (int j = 1; j < predicateRows.size(); j++) {
                int maxIdx = nextHighestRowInBucket(predicateRows.get(j),
                        row - 1, 0);
                if (maxIdx < 0) break ROW_LOOP;
                max = predicateRows.get(j).get(maxIdx);
                if (max > row) {
                    break;
                }
            }

            if (max == row) {
                result.add(row);
                i++;
                continue;
            }

            i = nextHighestRowInBucket(base, max - 1, i + 1);
            if (i < 0) break;
        }

        return Pair.of(result, maxRow);
    }
}
