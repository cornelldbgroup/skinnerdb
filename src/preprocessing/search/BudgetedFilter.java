package preprocessing.search;

import expressions.compilation.UnaryBoolEval;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.MutableIntList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

public class BudgetedFilter {
    private List<MutableIntList> resultList;
    private ImmutableList<UnaryBoolEval> compiled;
    private IndexFilter indexFilter;
    private int LAST_ROW;
    private boolean earlyFinish = false;

    public BudgetedFilter(ImmutableList<UnaryBoolEval> compiled,
                          IndexFilter indexFilter,
                          int LAST_ROW, List<MutableIntList> resultList) {
        this.resultList = resultList;
        this.compiled = compiled;
        this.indexFilter = indexFilter;
        this.LAST_ROW = LAST_ROW;
    }

    private long indexScan(int start,
                           MutableIntList result,
                           FilterState state) {

        long startTime = System.nanoTime();

        Pair<MutableIntList, Boolean> indexScanResult =
                indexFilter.getCandidateRowsFromIndex(state, start,
                        LAST_ROW);
        MutableIntList candidate = indexScanResult.getLeft();
        this.earlyFinish = indexScanResult.getRight();
        if (this.earlyFinish) {
            return 0;
        }

        IntIterator iterator = candidate.intIterator();

        if (state.cachedEval != null) {
            ROW_LOOP:
            while (iterator.hasNext()) {
                int row = iterator.next();

                if (state.cachedEval.evaluate(row) <= 0) {
                    continue ROW_LOOP;
                }

                for (int j = state.cachedTil + 1; j < state.order.length; j++) {
                    UnaryBoolEval expr = compiled.get(state.order[j]);
                    if (expr.evaluate(row) <= 0) {
                        continue ROW_LOOP;
                    }
                }

                result.add(row);
            }
        } else {
            ROW_LOOP:
            while (iterator.hasNext()) {
                int row = iterator.next();

                for (int j = state.indexedTil + 1; j < state.order.length;
                     j++) {
                    UnaryBoolEval expr = compiled.get(state.order[j]);
                    if (expr.evaluate(row) <= 0) {
                        continue ROW_LOOP;
                    }
                }

                result.add(row);
            }
        }

        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private long tableScanBranching(int begin, List<MutableIntList> result,
                                    FilterState state) {
        long startTime = System.nanoTime();

        for (int b = 0; b < state.batches; b++) {
            final int start = begin + b * state.batchSize;
            final int end = Math.min(start + state.batchSize, LAST_ROW);
            final MutableIntList batchResult = result.get(b);

            if (state.cachedEval != null) {
                ROW_LOOP:
                for (int row = start; row < end; row++) {
                    if (state.cachedEval.evaluate(row) <= 0) {
                        continue ROW_LOOP;
                    }

                    for (int i = state.cachedTil + 1;
                         i < state.order.length; i++) {
                        UnaryBoolEval expr =
                                compiled.get(state.order[i]);
                        if (expr.evaluate(row) <= 0) {
                            continue ROW_LOOP;
                        }
                    }

                    batchResult.add(row);
                }
            } else {
                ROW_LOOP:
                for (int row = start; row < end; row++) {
                    for (int predIndex : state.order) {
                        UnaryBoolEval expr = compiled.get(predIndex);
                        if (expr.evaluate(row) <= 0) {
                            continue ROW_LOOP;
                        }
                    }

                    batchResult.add(row);
                }
            }
        }

        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private long tableScanBitset(int start, MutableIntList result,
                                 FilterState state) {
        long startTime = System.nanoTime();
        int end = Math.min(start + state.batchSize * state.batches,
                LAST_ROW);
        int nrRows = end - start;

        List<BitSet> predicateEval = new ArrayList<>(compiled.size());
        for (int i = 0; i < compiled.size(); i++) {
            predicateEval.add(new BitSet(nrRows));
        }

        for (int row = start, i = 0; row < end; row++, i++) {
            for (int j = 0; j < compiled.size(); j++) {
                predicateEval.get(j).set(i, compiled.get(j).evaluate(row) > 0);
            }
        }

        BitSet filter = new BitSet(nrRows);
        filter.set(0, nrRows);
        for (BitSet pred : predicateEval) {
            filter.and(pred);
        }

        for (int row = start, i = 0; row < end; row++, i++) {
            if (filter.get(i)) {
                result.add(row);
            }
        }

        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    public double execute(int start, List<Integer> outputIds,
                          FilterState state) {
        long time;

        List<MutableIntList> outputs = new ArrayList<>(outputIds.size());
        for (int outId : outputIds) {
            outputs.add(resultList.get(outId));
        }

        int delta = Math.min(start + state.batches * state.batchSize,
                LAST_ROW) - start;

        if (state.indexedTil >= 0) {
            time = indexScan(start, outputs.get(0), state);
        } else if (state.avoidBranching) {
            time = tableScanBitset(start, outputs.get(0), state);
        } else {
            time = tableScanBranching(start, outputs, state);
        }


        return Math.exp(-(0.0001 * time) / delta);
    }

    public Collection<MutableIntList> getResult() {
        return resultList;
    }

    public boolean shouldEarlyFinish() {
        return earlyFinish;
    }
}
