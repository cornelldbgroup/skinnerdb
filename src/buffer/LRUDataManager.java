package buffer;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import config.GeneralConfig;
import config.LoggingConfig;
import data.*;
import diskio.DiskUtil;
import diskio.PathUtil;
import query.ColumnRef;
import types.TypeUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static buffer.BufferManager.log;

public class LRUDataManager implements IDataManager {
    /**
     * the LRU cache for loaded columns.
     */
    private Map<ColumnRef, ColumnData> colToData;
    private Stack<ColumnRef> columnOrder;
    private int capacity = GeneralConfig.cacheSize;
    private int size = 0;

    public LRUDataManager() {
        colToData = new ConcurrentHashMap<>();
        columnOrder = new Stack<>();
    }

    private void put(ColumnRef column, ColumnData data){
        // the column has been stored in the cache or the cache is full?
        if(colToData.containsKey(column)) {
            columnOrder.removeElement(column);
            columnOrder.add(column);
        }
        else {
            while (capacity < size + data.cardinality) {
                ColumnRef removeCol = columnOrder.firstElement();
                size -= colToData.get(removeCol).cardinality;
                columnOrder.removeElement(removeCol);
                colToData.remove(removeCol);
            }
            colToData.put(column, data);
            columnOrder.add(column);
            size += data.cardinality;
        }
    }

    public ColumnData get(ColumnRef column){
        if (colToData.containsKey(column)) {
            ColumnData data = colToData.get(column);
            put(column, data);
            return data;
        }
        else {
            return null;
        }
    }

    @Override
    public ColumnData getData(ColumnRef columnRef) throws Exception {
        // Load data if necessary
        ColumnData columnData = colToData.get(columnRef);
        if (columnData == null) {
            columnData = loadColumn(columnRef);
            put(columnRef, columnData);
        }
        return columnData;
    }

    private ColumnData loadColumn(ColumnRef columnRef) throws Exception {
        long startMillis = System.currentTimeMillis();
        // Get column information from catalog
        ColumnInfo column = CatalogManager.getColumn(columnRef);
        log("Loaded column meta-data: " + column.toString());
        // Read generic object from file
        String dataPath = PathUtil.colToPath.get(column);
        ColumnData columnData = DiskUtil.loadObject(dataPath, TypeUtil.toJavaType(column.type));
        // store column data into the memory
        put(columnRef, columnData);
        // Generate statistics for output
        if (LoggingConfig.BUFFER_VERBOSE) {
            long totalMillis = System.currentTimeMillis() - startMillis;
            System.out.println("Loaded " + columnRef.toString() +
                    " in " + totalMillis + " milliseconds");
        }
        // Generate debugging output
        log("*** Column " + columnRef.toString() + " sample ***");
        int cardinality = colToData.get(columnRef).getCardinality();
        int sampleSize = Math.min(10, cardinality);
        for (int i=0; i<sampleSize; ++i) {
            switch (column.type) {
                case STRING_CODE:
                    int code = ((IntData)columnData).data[i];
                    log(BufferManager.dictionary.getString(code));
                    break;
            }
        }
        log("******");
        return columnData;
    }
}
