package buffer;

import config.BufferConfig;
import config.GeneralConfig;
import config.LoggingConfig;
import data.*;
import indexing.Index;
import indexing.IntIndex;
import query.ColumnRef;
import statistics.BufferStats;
import statistics.JoinStats;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class LRUDataManager implements IDataManager {
    /**
     * the order of column touched by the system.
     */
    private Deque<Integer> columnOrder;
    /**
     * the LRU cache for loaded columns.
     */
    private Map<EntryRef, int[]> colToIndex;
//    private Table<Integer, Integer, EntryRef> entryCache;
    /**
     * the order of column touched by the system.
     */
    private Deque<EntryRef> indexOrder;
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

    public LRUDataManager() {
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
        // Load data if necessary
        return null;
    }

    private boolean checkSize() {
        int checkSize = 0;
        for (Integer integer : columnOrder) {
            IntIndex index = BufferManager.idToIndex.get(integer);
            checkSize += index.prefixSum * 4;
        }
        if (checkSize != size) {
            checkSize = 0;
            for (Integer integer : columnOrder) {
                IntIndex index = BufferManager.idToIndex.get(integer);
                checkSize += index.prefixSum * 4;
                System.out.println(index.prefixSum * 4 + " " + checkSize);
            }
            return false;
        }
        return true;
    }

    private boolean checkWindowSize() {
        int checkSize = 0;
        for (Integer index : columnOrder) {
            IntIndex intIndex = BufferManager.idToIndex.get(index);
            for (Integer startID: intIndex.loadedStartID) {
                checkSize += intIndex.entryRefs[startID].positions.length * 4;
            }
        }
        if (checkSize != size) {

            return false;
        }
        return true;
    }

    @Override
    public int getIndexData(IntIndex intIndex, int pos) throws Exception {
        int[] positions;
        int cid = intIndex.cid;
        BufferStats.nrIndexLookups++;
        if (intIndex.positions != null) {
            // cache hit
            BufferStats.nrCacheHit++;
            positions = intIndex.positions;
//            log("Column " + intIndex.cid + " is in the memory.");
            columnOrder.remove(cid);
            columnOrder.addFirst(cid);
//            log("Cache Lookup: " + BufferStats.nrIndexLookups +
//                    "\tCache Hits: " + BufferStats.nrCacheHit + "\tCache Miss: " + BufferStats.nrCacheMiss);
            return positions[pos];
        }
        // cache miss
        BufferStats.nrCacheMiss++;
        int prefixSum = intIndex.prefixSum;
        int indexSize = prefixSum * 4;
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
//        positions = intIndex.test;
        // the column has been stored in the cache or the cache is full?
        while (capacity < size + indexSize && size > 0) {
            int removeID = columnOrder.removeLast();
            IntIndex removeIndex = BufferManager.idToIndex.get(removeID);
            int colSize = removeIndex.prefixSum * 4;
            size -= colSize;
            log("Remove " + removeIndex.cid + " from the memory... The size: " + colSize);
            log("Buffer: " + size);
            removeIndex.positions = null;
        }
        intIndex.positions = positions;
        columnOrder.addFirst(cid);
        size += positions.length * 4;
        log("Returning Data... The cache size: " + size);
//        System.out.println("Cache Lookup: " + BufferStats.nrIndexLookups +
//                "\tCache Hits: " + BufferStats.nrCacheHit + "\tCache Miss: " + BufferStats.nrCacheMiss);
        log("Cache Lookup: " + BufferStats.nrIndexLookups +
                "\tCache Hits: " + BufferStats.nrCacheHit + "\tCache Miss: " + BufferStats.nrCacheMiss);
        return positions[pos];
    }

    @Override
    public int getDataInWindow(IntIndex intIndex, int pos) throws Exception {
        BufferStats.nrIndexLookups++;
        int cid = intIndex.cid;
        int length = BufferConfig.pageSize / 4;
        int startIndex = pos / length;
        int start = startIndex * length;
        int offset = pos - start;

        EntryRef entryRef = intIndex.entryRefs[startIndex];
        int channelLength = intIndex.prefixSum - start;
        length = Math.min(length, channelLength);
        int indexSize = length * 4;
        if (entryRef.positions != null) {
            // cache hit
            BufferStats.nrCacheHit++;
            JoinStats.cacheMiss = false;
//            log("Column " + intIndex.cid + " starting from " + start + " is in the memory.");
            // move the index entry to the first.
            columnOrder.remove(cid);
            columnOrder.addFirst(cid);
//            log("Cache Lookup: " + BufferStats.nrIndexLookups +
//                    "\t Cache Hits: " + BufferStats.nrCacheHit + "\t Cache Miss: " + BufferStats.nrCacheMiss);
//            return intIndex.test[pos];
            return entryRef.positions[offset];
        }
        // cache miss
        BufferStats.nrCacheMiss++;
        JoinStats.cacheMiss = true;
        int[] positions = new int[length];
        // load data from the buffer
        SeekableByteChannel channel = intIndex.positionChannel;
        log("Load " + intIndex.cid + " from the disk... The size: " + indexSize);
        channel = channel.position(start * 4);
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
            int removeID = columnOrder.removeLast();
            IntIndex removeIndex = BufferManager.idToIndex.get(removeID);
            Iterator<Integer> iterator = removeIndex.loadedStartID.iterator();
            while (iterator.hasNext()) {
                Integer removeStartIndex = iterator.next();
                EntryRef removeEntry = removeIndex.entryRefs[removeStartIndex];
                int colSize = removeEntry.positions.length * 4;
                removeEntry.positions = null;
                size -= colSize;
                iterator.remove();
                if (capacity >= size + indexSize || size == 0) {
                    break;
                }
            }
            if (removeIndex.loadedStartID.size() > 0) {
                columnOrder.addLast(removeID);
            }
        }
        // push the index entry to the first.
        if (intIndex.loadedStartID.size() == 0) {
            columnOrder.addFirst(cid);
        }
        intIndex.entryRefs[startIndex].positions = positions;
        intIndex.loadedStartID.add(startIndex);
        size += length * 4;

        log("Returning Data... The cache size: " + size);
        log("Cache Lookup: " + BufferStats.nrIndexLookups +
                "\t Cache Hits: " + BufferStats.nrCacheHit + "\t Cache Miss: " + BufferStats.nrCacheMiss);
//        return intIndex.test[pos];
        return positions[offset];
    }

    @Override
    public void clearCache() {
        colToIndex.clear();
        columnOrder.clear();
        indexOrder.clear();
        BufferManager.colToIndex.values().forEach(Index::clear);
        size = 0;
    }

    @Override
    public void log(String text) {
        if (LoggingConfig.MANAGER_VERBOSE) {
            try {
//                writer.write(text);
//                writer.write("\n");
                System.out.println(text);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void unloadIndex(IntIndex intIndex) {
        boolean isMoved = columnOrder.remove(intIndex.cid);
        if (isMoved) {
            size -= intIndex.prefixSum * 4;
        }
    }


}
