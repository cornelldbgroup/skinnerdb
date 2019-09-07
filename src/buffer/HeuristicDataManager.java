package buffer;

import config.BufferConfig;
import config.GeneralConfig;
import config.LoggingConfig;
import data.ColumnData;
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

public class HeuristicDataManager implements IDataManager {
    /**
     * order list
     */
    private Deque<Integer>[] orders;
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

    public HeuristicDataManager() {

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
        return 0;
    }

    @Override
    public int getDataInWindow(IntIndex intIndex, int pos) throws Exception {
        BufferStats.nrIndexLookups++;
        int cid = intIndex.cid;
        int tid = intIndex.tid;
        Deque<Integer> tableOrder = orders[tid];

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
            tableOrder.remove(cid);
            tableOrder.addFirst(cid);
//            log("Column " + intIndex.cid + " starting from " + start + " is in the memory.");
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
        removeElements(indexSize);
        // push the index entry to the order position.
        addElements(intIndex, startIndex, positions);

        log("Returning Data... The cache size: " + size);
        log("Cache Lookup: " + BufferStats.nrIndexLookups +
                "\t Cache Hits: " + BufferStats.nrCacheHit + "\t Cache Miss: " + BufferStats.nrCacheMiss);
//        return intIndex.test[pos];
        return positions[offset];
    }


    @Override
    public void clearCache() {
        BufferManager.colToIndex.values().forEach(Index::clear);
        size = 0;
    }

    @Override
    public void log(String text) {
        if (LoggingConfig.MANAGER_VERBOSE) {
            try {
                writer.write(text);
                writer.write("\n");
//                System.out.println(text);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void unloadIndex(IntIndex intIndex) {

    }


    private void addElements(IntIndex intIndex, int startIndex, int[] positions) {
        int tid = intIndex.tid;
        int cid = intIndex.cid;
        Deque<Integer> tableOrder = orders[tid];

        if (intIndex.pageOrder.size() == 0) {
            tableOrder.addFirst(cid);
        }
        else {
            tableOrder.remove(cid);
            tableOrder.addFirst(cid);
        }
        intIndex.pageOrder.addLast(startIndex);
        intIndex.entryRefs[startIndex].positions = positions;
        size += positions.length * 4;
    }

    private void removeElements(int indexSize) {
        while (capacity < size + indexSize && size > 0) {
            int removeID = -1;
            Deque<Integer> deque = null;
            for (int i = 0; i < JoinStats.order.length; i++) {
                int tid = JoinStats.order[i];
                deque = orders[tid];
                if (deque.size() > 0) {
                    removeID = deque.removeLast();
                }
            }
            IntIndex removeIndex = BufferManager.idToIndex.get(removeID);
            while (removeIndex.pageOrder.size() > 0) {
                Integer removeStartIndex = removeIndex.pageOrder.removeFirst();
                EntryRef removeEntry = removeIndex.entryRefs[removeStartIndex];

                int colSize = removeEntry.positions.length * 4;
                removeEntry.positions = null;
                size -= colSize;
                if (capacity >= size + indexSize || size == 0) {
                    break;
                }
            }
            if (removeIndex.pageOrder.size() > 0) {
                deque.addLast(removeID);
            }
        }
    }

    public void setup(int nrTables) {
        orders = new LinkedList[nrTables];
        Arrays.fill(orders, new LinkedList<>());
    }

}
