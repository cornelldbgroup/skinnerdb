package indexing;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.IndexingMode;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import parallel.ParallelService;
import query.ColumnRef;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Features utility functions for creating indexes.
 *
 * @author immanueltrummer
 */
public class Indexer {
    /**
     * Create an index on the specified column.
     *
     * @param colRef create index on this column
     */
    public static void index(ColumnRef colRef) throws Exception {
        // Check if index already exists
        if (!BufferManager.colToIndex.containsKey(colRef)) {
            ColumnData data = BufferManager.getData(colRef);
            if (data instanceof IntData) {
                IntData intData = (IntData) data;
                HashIntIndex index = new HashIntIndex(intData);
                BufferManager.colToIndex.put(colRef, index);
            } else if (data instanceof DoubleData) {
                DoubleData doubleData = (DoubleData) data;
                HashDoubleIndex index = new HashDoubleIndex(doubleData);
                BufferManager.colToIndex.put(colRef, index);
            }
        }
    }

    /**
     * Creates an index for each key/foreign key column.
     *
     * @param mode determines on which columns to create indices
     * @throws Exception
     */
    public static void indexAll(IndexingMode mode) throws Exception {
        System.out.println("Indexing all key columns ...");
        long startMillis = System.currentTimeMillis();

        List<Future> futures = new ArrayList<>();
        for (TableInfo tableI : CatalogManager.currentDB.nameToTable.values()) {
            for (ColumnInfo columnInfo : tableI.nameToCol.values()) {
                futures.add(ParallelService.POOL.submit(() -> {
                    try {
                        if (mode.equals(IndexingMode.ALL) ||
                                (mode.equals(IndexingMode.ONLY_KEYS) &&
                                        (columnInfo.isPrimary || columnInfo.isForeign))) {
                            String table = tableI.name;
                            String column = columnInfo.name;
                            ColumnRef colRef =
                                    new ColumnRef(table, column);
                            System.out.println("Indexing " + colRef + " ." +
                                    "..");
                            index(colRef);
                        }
                    } catch (Exception e) {
                        System.err.println("Error indexing " + columnInfo);
                        e.printStackTrace();
                    }
                }));
            }
        }

        for (Future f : futures) f.get();

        long totalMillis = System.currentTimeMillis() - startMillis;
        System.out.println("Indexing took " + totalMillis + " ms.");
    }
}
