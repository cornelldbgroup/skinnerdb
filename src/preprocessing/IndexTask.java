package preprocessing;

import buffer.BufferManager;
import catalog.info.ColumnInfo;
import config.GeneralConfig;
import config.LoggingConfig;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import indexing.Index;
import indexing.Indexer;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IndexPolicy;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import query.ColumnRef;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class IndexTask extends RecursiveAction {
    public final ColumnRef colRef;
    public final ColumnRef queryRef;
    public final PartitionIndex oldIndex;
    public final int nrThreads;
    public final boolean isPrimary;
    public final boolean isSeq;
    public final boolean sorted;

    public IndexTask(ColumnRef colRef, ColumnRef queryRef, PartitionIndex oldIndex, int nrThreads,
                     boolean isPrimary, boolean isSeq, boolean sorted) {
        this.colRef = colRef;
        this.queryRef = queryRef;
        this.oldIndex = oldIndex;
        this.nrThreads = nrThreads;
        this.isPrimary = isPrimary;
        this.isSeq = isSeq;
        this.sorted = sorted;
    }


    @Override
    protected void compute() {
        // The index is not in the buffer
        if (!BufferManager.colToIndex.containsKey(colRef)) {
            ColumnData data = null;
            try {
                data = BufferManager.getData(colRef);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (data instanceof IntData) {
                IntData intData = (IntData)data;
                IntPartitionIndex intIndex = oldIndex == null ? null : (IntPartitionIndex) oldIndex;
                int keySize = intIndex == null ? 0 : intIndex.keyToPositions.size();
                IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, intData.cardinality);
                IntPartitionIndex index = new IntPartitionIndex(intData, nrThreads, colRef, queryRef,
                        intIndex, policy);
                BufferManager.colToIndex.put(colRef, index);
            } else if (data instanceof DoubleData) {
                DoubleData doubleData = (DoubleData)data;
                DoublePartitionIndex doubleIndex = oldIndex == null ? null : (DoublePartitionIndex) oldIndex;
                int keySize = doubleIndex == null ? 0 : doubleIndex.keyToPositions.size();
                IndexPolicy policy = Indexer.indexPolicy(isPrimary, isSeq, keySize, doubleData.cardinality);
                DoublePartitionIndex index = new DoublePartitionIndex(doubleData, nrThreads,
                        colRef, queryRef, doubleIndex, policy);
                BufferManager.colToIndex.put(colRef, index);
            }
        }
    }
}
