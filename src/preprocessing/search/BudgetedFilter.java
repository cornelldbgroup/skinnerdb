package preprocessing.search;

import catalog.CatalogManager;
import expressions.compilation.UnaryBoolEval;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import parallel.ParallelService;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

public class BudgetedFilter {
    private final int LAST_TABLE_ROW;
    private int lastCompletedRow;
    private List<MutableIntList> resultList;
    private ImmutableList<UnaryBoolEval> compiled;
    private IndexFilter indexFilter;


    public BudgetedFilter(String tableName,
                          ImmutableList<UnaryBoolEval> compiled,
                          IndexFilter indexFilter) {
        this.resultList = new ArrayList<>();
        this.compiled = compiled;
        this.LAST_TABLE_ROW = CatalogManager.getCardinality(tableName) - 1;
        this.lastCompletedRow = -1;
        this.indexFilter = indexFilter;
    }


    private Pair<Long, Integer> indexScan(int budget,
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

                for (int j = state.indexedTil + 1; j < state.order.length; j++) {
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
        long duration = endTime - startTime;
        return Pair.of(duration, pair.getValue());
    }

    private Pair<Long, Integer> tableScanBranchingParallel(int budgetPerThread,
                                                           FilterState state) {
        long startTime = System.nanoTime();
        int endRow = lastCompletedRow;
        List<Future<MutableIntList>> futures = new ArrayList<>();

        for (int j = 0; j < state.parallelBatches; j++) {
            final int start = lastCompletedRow + budgetPerThread * j;
            if (start >= LAST_TABLE_ROW) break;
            final int end = Math.min(start + budgetPerThread, LAST_TABLE_ROW);
            endRow = end;

            if (state.cachedEval != null) {
                futures.add(ParallelService.HIGH_POOL.submit(() -> {
                    MutableIntList tempResult = IntLists.mutable.empty();

                    ROW_LOOP:
                    for (int row = start + 1; row <= end; row++) {
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

                        tempResult.add(row);
                    }

                    return tempResult;
                }));
            } else {
                futures.add(ParallelService.HIGH_POOL.submit(() -> {
                    MutableIntList tempResult = IntLists.mutable.empty();

                    ROW_LOOP:
                    for (int row = start + 1; row <= end; row++) {
                        for (int predIndex : state.order) {
                            UnaryBoolEval expr = compiled.get(predIndex);
                            if (expr.evaluate(row) <= 0) {
                                continue ROW_LOOP;
                            }
                        }

                        tempResult.add(row);
                    }

                    return tempResult;
                }));
            }
        }

        for (Future<MutableIntList> f : futures) {
            try {
                resultList.add(f.get());
            } catch (Exception e) {}
        }

        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, endRow);
    }

    private Pair<Long, Integer> tableScanBranching(int budget,
                                                   FilterState state) {
        long startTime = System.nanoTime();
        MutableIntList result = IntLists.mutable.empty();

        final int start = lastCompletedRow;
        final int end = Math.min(lastCompletedRow + budget, LAST_TABLE_ROW);

        if (state.cachedEval != null) {
            ROW_LOOP:
            for (int row = start + 1; row <= end; row++) {
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

                result.add(row);
            }
        } else {
            ROW_LOOP:
            for (int row = start + 1; row <= end; row++) {
                for (int predIndex : state.order) {
                    UnaryBoolEval expr = compiled.get(predIndex);
                    if (expr.evaluate(row) <= 0) {
                        continue ROW_LOOP;
                    }
                }

                result.add(row);
            }
        }

        resultList.add(result);
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, end);
    }

    private Pair<Long, Integer> tableScanBitset(int nrRows,
                                                FilterState state) {
        long startTime = System.nanoTime();
        MutableIntList result = IntLists.mutable.empty();

        int begin = lastCompletedRow + 1;
        int end = Math.min(lastCompletedRow + nrRows, LAST_TABLE_ROW);
        nrRows = end - begin + 1;

        List<BitSet> predicateEval = new ArrayList<>(compiled.size());
        for (int i = 0; i < compiled.size(); i++) {
            predicateEval.add(new BitSet(nrRows));
        }


        for (int row = begin, i = 0; row <= end; row++, i++) {
            for (int j = 0; j < compiled.size(); j++) {
                predicateEval.get(j).set(i, compiled.get(j).evaluate(row) > 0);
            }
        }

        BitSet filter = new BitSet(nrRows);
        filter.set(0, nrRows);
        for (BitSet pred : predicateEval) {
            filter.and(pred);
        }

        for (int row = begin, i = 0; row <= end; row++, i++) {
            if (filter.get(i)) {
                result.add(row);
            }
        }

        resultList.add(result);
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        return Pair.of(duration, end);
    }

    public double executeWithBudget(int budget, FilterState state) {
        Pair<Long, Integer> result;

        if (state.indexedTil >= 0) { // Use index to filter rows.
            result = indexScan(budget, state);
        } else if (state.avoidBranching) {
            result = tableScanBitset(budget, state);
        } else {
            if (state.parallelBatches > 0) {
                result = tableScanBranchingParallel(budget, state);
            } else {
                result = tableScanBranching(budget, state);
            }
        }

                

        int completedRow = result.getRight();
        double reward = (completedRow - lastCompletedRow) / (0.0001 * result.getLeft());
        lastCompletedRow = completedRow;
        System.out.println(state.toString() + " " + reward + " " + lastCompletedRow);
        return reward;
    }

    public boolean isFinished() {
        return lastCompletedRow == LAST_TABLE_ROW;
    }

    public Collection<MutableIntList> getResult() {
        return resultList;
    }
}
