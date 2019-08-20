package buffer;

import config.BufferConfig;
import config.GeneralConfig;
import config.LoggingConfig;
import data.ColumnData;
import indexing.IntIndex;
import query.ColumnRef;
import statistics.BufferStats;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class FIFODataManager implements IDataManager {
    /**
     * the order of column touched by the system.
     */
    private Queue<IntIndex> columnOrder;
    /**
     * the LRU cache for loaded columns.
     */
    private Map<EntryRef, int[]> colToIndex;
//    private Table<Integer, Integer, EntryRef> entryCache;
    /**
     * the order of column touched by the system.
     */
    private Queue<EntryRef> indexOrder;
    /**
     * the size of cache.
     */
    private int capacity = GeneralConfig.cacheSize;
    /**
     * the current size of data stored in the memory.
     */
    private int size = 0;
    /**
     * log writer
     */
    private BufferedWriter writer;

    public FIFODataManager() {
        colToIndex = new HashMap<>();
        columnOrder = new LinkedList<>();
        indexOrder = new LinkedList<>();
        if (LoggingConfig.MANAGER_VERBOSE) {
            try {
                writer = Files.newBufferedWriter(Paths.get("log.txt"), StandardCharsets.UTF_8);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public ColumnData getData(ColumnRef columnRef) throws Exception {
        return null;
    }

    @Override
    public int getIndexData(IntIndex intIndex, int pos) throws Exception {
        int[] positions;
        BufferStats.nrIndexLookups++;
        if (intIndex.positions != null) {
            // cache hit
            BufferStats.nrCacheHit++;
            positions = intIndex.positions;
            log("Column " + intIndex.cid + " is in the memory.");
            return positions[pos];
        }
        // cache miss
        BufferStats.nrCacheMiss++;
        int prefixSum = intIndex.prefixSum;
        int indexSize = prefixSum * 4;
        log("Load " + intIndex.cid + " from the disk... The size: " + indexSize);
        // load data from the buffer
        SeekableByteChannel channel = intIndex.positionChannel;
        channel = channel.position(0);
        positions = new int[prefixSum];
        // create byte buffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(indexSize);
        byteBuffer.order(ByteOrder.nativeOrder());
        channel.read(byteBuffer);
        byteBuffer.position(0);
        for (int i = 0; i < prefixSum; i++) {
            positions[i] = byteBuffer.getInt(4 * i);
        }
        // the column has been stored in the cache or the cache is full?
        while (capacity < size + indexSize && size > 0) {
            IntIndex removeIndex = columnOrder.remove();
            int colSize = removeIndex.prefixSum * 4;
            size -= colSize;
            log("Remove " + removeIndex.cid + " from the memory... The size: " + colSize);
            log("Buffer: " + size);
            removeIndex.positions = null;
        }
        intIndex.positions = positions;
        columnOrder.add(intIndex);
        size += positions.length * 4;
        log("Returning Data... The cache size: " + size);

        return positions[pos];
    }

    @Override
    public int getDataInWindow(IntIndex intIndex, int pos) throws Exception {
        int cid = intIndex.cid;
        BufferStats.nrIndexLookups++;
        int length = BufferConfig.pageSize / 4;
        int start = pos / length * length;
        int offset = pos - start;
        EntryRef entryRef = intIndex.keyToEntries.get(start);
        if (entryRef != null) {
            // cache hit
            BufferStats.nrCacheHit++;
            log("Column " + intIndex.cid + " starting from " + start + " is in the memory.");
            return entryRef.positions[offset];
        }
        // cache miss
        BufferStats.nrCacheMiss++;
        // load data from the buffer
        SeekableByteChannel channel = intIndex.positionChannel;
        int channelLength = (int) (channel.size() / 4);

        length = Math.min(length, channelLength);
        int indexSize = length * 4;
        log("Load " + intIndex.cid + " from the disk... The size: " + indexSize);
        channel = channel.position(start * 4);
        int[] positions = new int[length];
        // create byte buffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(indexSize);
        byteBuffer.order(ByteOrder.nativeOrder());
        channel.read(byteBuffer);
        byteBuffer.position(0);
        for (int i = 0; i < length; i++) {
            positions[i] = byteBuffer.getInt(4 * i);
        }
        // the column has been stored in the cache or the cache is full?
        while (capacity < size + indexSize && size > 0) {
            EntryRef removeEntry = indexOrder.remove();
            intIndex.keyToEntries.remove(start);
            int colSize = removeEntry.positions.length * 4;
            size -= colSize;
            log("Remove " + removeEntry.cid + " starting from " + start + " from the memory... The size: " + colSize);
            log("Buffer: " + size);
        }
        entryRef = new EntryRef(cid, start, positions);
        intIndex.keyToEntries.put(start, entryRef);
        // push the index entry to the first.
        indexOrder.add(entryRef);
        size += positions.length * 4;
        log("Returning Data... The cache size: " + size);
        return positions[offset];
    }

    @Override
    public void clearCache() {
        colToIndex.clear();
        columnOrder.clear();
        indexOrder.clear();
        size = 0;
    }

    @Override
    public void log(String text) {
        if (LoggingConfig.MANAGER_VERBOSE) {
            try {
                writer.write(text);
                writer.write("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
