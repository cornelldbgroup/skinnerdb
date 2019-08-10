package buffer;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import config.GeneralConfig;
import config.LoggingConfig;
import data.*;
import diskio.DiskUtil;
import diskio.PathUtil;
import indexing.IntIndex;
import query.ColumnRef;
import types.TypeUtil;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static buffer.BufferManager.log;

public class LRUDataManager implements IDataManager {
    /**
     * the LRU cache for loaded columns.
     */
    private Map<ColumnRef, ColumnData> colToData;
    /**
     * the LRU cache for loaded columns.
     */
    private Map<ColumnRef, int[]> colToIndex;
    /**
     * the order of column touched by the system.
     */
    private Deque<ColumnRef> columnOrder;
    /**
     * the size of cache.
     */
    private int capacity = GeneralConfig.cacheSize;
    /**
     * the current size of data stored in the memory.
     */
    private int size = 0;
    /**
     * the lock to guarantee the consistency of cache.
     */
    private Lock cacheLock;

    public LRUDataManager() {
        colToData = new HashMap<>();
        colToIndex = new HashMap<>();
        columnOrder = new LinkedList<>();
        cacheLock = new ReentrantLock();
    }

    private void put(ColumnRef column, ColumnData data){
        // the column has been stored in the cache or the cache is full?
        if(colToData.containsKey(column)) {
            columnOrder.remove(column);
            columnOrder.addFirst(column);
        }
        else {
            while (capacity < size + data.cardinality || size == 0) {
                ColumnRef removeCol = columnOrder.removeLast();
                size -= colToData.get(removeCol).cardinality;
                colToData.remove(removeCol);
            }
            colToData.put(column, data);
            columnOrder.addFirst(column);
            size += data.cardinality;
        }
    }

    private void putIndex(ColumnRef column, int[] positions){
        // the column has been stored in the cache or the cache is full?
        if(colToIndex.containsKey(column)) {
            columnOrder.remove(column);
            columnOrder.addFirst(column);
        }
        else {
            while (capacity < size + positions.length && size > 0) {
                ColumnRef removeCol = columnOrder.removeLast();
                size -= colToIndex.get(removeCol).length;
                colToIndex.remove(removeCol);
            }
            colToIndex.put(column, positions);
            columnOrder.addFirst(column);
            size += positions.length;
        }
    }

    @Override
    public ColumnData getData(ColumnRef columnRef) throws Exception {
        // Load data if necessary
        ColumnData columnData = colToData.get(columnRef);
        if (columnData == null) {
            columnData = loadColumn(columnRef);
        }
        cacheLock.lock();
        put(columnRef, columnData);
        cacheLock.unlock();
        return columnData;
    }

    @Override
    public int getIndexData(IntIndex intIndex, int pos) throws Exception {
        ColumnRef columnRef = intIndex.columnRef;
        int[] positions;
        if (!colToIndex.containsKey(columnRef)) {
            SeekableByteChannel channel = intIndex.positionChannel;
            channel = channel.position(0);
            int prefixSum = intIndex.prefixSum;
            positions = new int[prefixSum];
            // create byte buffer
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 * prefixSum);
            channel.read(byteBuffer);
            byteBuffer.position(0);
            IntBuffer intBuffer = byteBuffer.asIntBuffer();
            intBuffer.get(positions);
        }
        else {
            positions = colToIndex.get(columnRef);
        }
        putIndex(columnRef, positions);
        return positions[pos];
    }


    private ColumnData loadColumn(ColumnRef columnRef) throws Exception {
        long startMillis = System.currentTimeMillis();
        // Get column information from catalog
        ColumnInfo column = CatalogManager.getColumn(columnRef);
        log("Loaded column meta-data: " + column.toString());
        // Read generic object from file
        String dataPath = PathUtil.colToPath.get(column);
        ColumnData columnData = DiskUtil.loadObject(dataPath, TypeUtil.toJavaType(column.type));
        // Generate statistics for output
        if (LoggingConfig.BUFFER_VERBOSE) {
            long totalMillis = System.currentTimeMillis() - startMillis;
            System.out.println("Loaded " + columnRef.toString() +
                    " in " + totalMillis + " milliseconds");
        }
        // Generate debugging output
        log("*** Column " + columnRef.toString() + " sample ***");
        int cardinality = columnData.getCardinality();
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
