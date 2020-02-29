package preprocessing.search;

import expressions.compilation.UnaryBoolEval;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import parallel.ParallelService;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

public class BudgetedFilter {
    private List<MutableIntList> resultList;
    private ImmutableList<UnaryBoolEval> compiled;
    private IndexFilter indexFilter;
    private int CARDINALITY;

    public BudgetedFilter(ImmutableList<UnaryBoolEval> compiled,
                          IndexFilter indexFilter,
                          int CARDINALITY) {
        this.resultList = new ArrayList<>();
        this.compiled = compiled;
        this.indexFilter = indexFilter;
        this.CARDINALITY = CARDINALITY;
    }


    /*private long indexScan(int budget,
                           List<MutableIntList> outputs,
                           FilterState state) {

        long startTime = System.nanoTime();
        MutableIntList result = IntLists.mutable.empty();

        Pair<MutableIntList, Integer> pair = indexFilter.getRows(budget,
                state, lastCompletedRow, LAST_TABLE_ROW);

        IntList candidate = pair.getKey();

        if (state.cachedEval != null) {
            ROW_LOOP:
            for (int i = 0; i < candidate.size(); i++) {
                int row = candidate.get(i);

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
            for (int i = 0; i < candidate.size(); i++) {
                int row = candidate.get(i);

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

        resultList.add(result);
        long endTime = System.nanoTime();
        return endTime - startTime;
    }*/

    private long tableScanBranching(int begin, List<MutableIntList> result,
                                    FilterState state) {
        long startTime = System.nanoTime();
        List<Runnable> actions = new ArrayList<>();

        for (int b = 0; b < state.batches; b++) {
            final int start = begin + b * state.batchSize;
            final int end = Math.min(start + state.batchSize, CARDINALITY);
            final MutableIntList batchResult = result.get(b);

            if (state.cachedEval != null) {
                actions.add(() -> {
                    ROW_LOOP:
                    for (int row = start; row < end; row++) {
                        if (state.cachedEval.evaluate(row) <= 0) {
                            continue ROW_LOOP;
                        }

                        for (int i = state.cachedTil + 1;
                             i < state.order.length; i++) {
                            UnaryBoolEval expr = compiled.get(state.order[i]);
                            if (expr.evaluate(row) <= 0) {
                                continue ROW_LOOP;
                            }
                        }

                        batchResult.add(row);
                    }
                });
            } else {
                actions.add(() -> {
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
                });
            }

            if (end == CARDINALITY) break;
        }

        List<Future> futures = new ArrayList<>();
        for (Runnable r : actions) {
            futures.add(ParallelService.HIGH_POOL.submit(r));
        }

        for (Future f : futures) {
            try {
                f.get();
            } catch (Exception e) {}
        }

        long endTime = System.nanoTime();
        return endTime - startTime;
    }

    private long tableScanBitset(int start, MutableIntList result,
                                 FilterState state) {
        long startTime = System.nanoTime();
        int end = Math.min(start + state.batchSize * state.batches,
                CARDINALITY);
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

        /*if (state.indexedTil >= 0) {
            time = indexScan(start, outputs, state);
        } else*/
        if (state.avoidBranching) {
            time = tableScanBitset(start, outputs.get(0), state);
        } else {
            time = tableScanBranching(start, outputs, state);
        }

        int delta = Math.min(start + state.batches * state.batchSize,
                CARDINALITY) - start;
        return delta / (0.0001 * time);
    }

    public List<Integer> initializeEpoch(int numEpochs) {
        List<Integer> outputIds = new ArrayList<>(numEpochs);

        for (int i = 0; i < numEpochs; i++) {
            resultList.add(IntLists.mutable.empty());
            outputIds.add(resultList.size() - 1);
        }

        return outputIds;
    }

    public Collection<MutableIntList> getResult() {
        return resultList;
    }
}
