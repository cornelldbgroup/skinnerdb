package indexing;

import buffer.BufferManager;
import buffer.EntryRef;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import com.sun.tools.javah.Gen;
import config.BufferConfig;
import config.CheckConfig;
import config.GeneralConfig;
import config.LoggingConfig;
import data.IntData;
import diskio.PathUtil;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import preprocessing.Preprocessor;
import query.ColumnRef;
import statistics.BufferStats;
import statistics.JoinStats;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Indexes integer values (not necessarily unique).
 *
 * @author immanueltrummer
 */
public class IntIndex extends Index {
    /**
     * Integer data that the index refers to.
     */
    public final IntData intData;
    /**
     * Reference to a specific column.
     */
    public final ColumnRef columnRef;
    /**
     * Reference to a specific column id.
     */
    public final int cid;
    /**
     * Reference to a specific table id.
     */
    public int tid;
    /**
     * Cardinality of indexed table.
     */
    public final int cardinality;
    /**
     * After indexing: contains for each search key
     * the number of entries, followed by the row
     * numbers at which those entries are found.
     */
    public int[] positions;
    /**
     * Test array that is equivalent to original positions
     */
    public int[] test;
    /**
     * Number of position information
     */
    public int prefixSum;
    /**
     * Number of pages
     */
    public int nrPages;
    /**
     * After indexing: maps search key to index
     * of first position at which associated
     * information is stored.
     */
    public IntIntMap keyToPositions;
    public IntIntMap keyToNumber;
    public Map<Integer, int[]> keyToMeta;
    /**
     * Local memory saving a mapping from
     * distinct value to part of positions
     * that is encapsulated in EntryRef.
     */
    public EntryRef[] entryRefs;
    /**
     * Set of pages that are stored in the memory currently.
     */
    public Set<Integer> loadedStartID;
    /**
     * Order of pages that are stored in the memory currently.
     */
    public Deque<Integer> pageOrder;
    /**
     * After indexing: if inMemory is not enable,
     * store the positions array to the file by java NIO.
     */
    public SeekableByteChannel positionChannel;
    /**
     * Position of current iterator.
     */
    int iterPos = -1;
    /**
     * Last valid position for current iterator.
     */
    int lastIterPos = -1;

    /**
     * Create index on the given integer column.
     *
     * @param colRef  create index on this column
     * @param intData integer data to index
     */
    public IntIndex(ColumnRef colRef, IntData intData) {
        long startMillis = System.currentTimeMillis();
        // Extract info
        this.intData = intData;
        this.cardinality = intData.cardinality;
        this.columnRef = colRef;
        this.cid = BufferManager.colToID.get(colRef);
        int pageLen = BufferConfig.pageSize / 4;
        // load data from the disk
        Path indexPath = Paths.get(PathUtil.indexPath, columnRef.toString() + ".idx");
        Map<Integer, List<Integer>> groupByValue = new HashMap<>();

        // load keys map
        int[] data = intData.data;
        // Count number of occurrences for each value
        for (int i = 0; i < cardinality; ++i) {
            // Don't index null values
            if (!intData.isNull.get(i)) {
                int value = data[i];
                groupByValue.putIfAbsent(value, new ArrayList<>());
                List<Integer> valueList = groupByValue.get(value);
                valueList.add(i);
            }
        }
        // Assign each key to the appropriate position offset
        int nrKeys = groupByValue.size();
        log("Number of keys:\t" + nrKeys);
        keyToPositions = HashIntIntMaps.newMutableMap(nrKeys);
        keyToNumber = HashIntIntMaps.newMutableMap(nrKeys);
        keyToMeta = new HashMap<>();
        loadedStartID = new HashSet<>();
        pageOrder = new LinkedList<>();
        int prefixSum = 0;

        for (Map.Entry<Integer, List<Integer>> entry: groupByValue.entrySet()) {
            int key = entry.getKey();
            List<Integer> valueList = entry.getValue();
            keyToPositions.put(key, prefixSum);
            int nrValues = valueList.size();
            keyToNumber.put(key, nrValues);
            // Advance offset taking into account
            // space for row indices and one field
            // storing the number of following indices.
            int nrFields = nrValues + 1;
            // update key to meta
            int firstPos = prefixSum + 1;
            int endPos = prefixSum + nrValues;
            int pageStart = firstPos / pageLen;
            int pageEnd = endPos / pageLen;
            int nrPages = pageEnd - pageStart + 1;
            int[] meta = new int[nrPages + 1];
            meta[0] = valueList.get(0);
            int start = (pageStart + 1) * pageLen - firstPos;
            for (int i = 1; i < nrPages; i++) {
                meta[i] = valueList.get(start);
                start += pageLen;
            }
            meta[nrPages] = valueList.get(nrValues - 1);
            keyToMeta.put(key, meta);
            prefixSum += nrFields;
        }

        // Generate position information
        if (CheckConfig.INDEXING_TEST) {
            int[] positions = new int[prefixSum];
            for (int i = 0; i < cardinality; ++i) {
                if (!intData.isNull.get(i)) {
                    int key = data[i];
                    int startPos = keyToPositions.get(key);
                    positions[startPos] += 1;
                    int offset = positions[startPos];
                    int pos = startPos + offset;
                    positions[pos] = i;
                }
            }
            test = positions;
        }

        this.prefixSum = prefixSum;
        int nrEntry = prefixSum / pageLen + 1;
        nrPages = nrEntry;
        entryRefs = new EntryRef[nrEntry];
        for (int i = 0; i < entryRefs.length; i++) {
            entryRefs[i] = new EntryRef(cid, i, null);
        }

        if (Files.exists(indexPath) && !GeneralConfig.indexInMemory) {
            // load positions channel
            Set<StandardOpenOption> options = new HashSet<>();
            options.add(StandardOpenOption.READ);
            options.add(StandardOpenOption.WRITE);
            try {
                positionChannel = Files.newByteChannel(indexPath, options);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // Generate position information
            int[] positions = new int[prefixSum];
            for (int i = 0; i < cardinality; ++i) {
                if (!intData.isNull.get(i)) {
                    int key = data[i];
                    int startPos = keyToPositions.get(key);
                    positions[startPos] += 1;
                    int offset = positions[startPos];
                    int pos = startPos + offset;
                    positions[pos] = i;
                }
            }
            if (CheckConfig.INDEXING_TEST) {
                test = positions;
            }
            // Check index if enabled
            IndexChecker.checkIndex(intData, this);
            // write indexes to files when the inMemory is not enable.
            if (!GeneralConfig.indexInMemory) {
                try {
                    store(positions);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                this.positions = positions;
            }
        }
        // Output statistics for performance tuning
        if (LoggingConfig.INDEXING_VERBOSE) {
            long totalMillis = System.currentTimeMillis() - startMillis;
            log("Created index for integer column with cardinality " +
                    cardinality + " in " + totalMillis + " ms.");
        }
        BufferManager.idToIndex.putIfAbsent(cid, this);
    }

    /**
     * Returns index of next tuple with given value
     * or cardinality of indexed table if no such
     * tuple exists.
     *
     * @param value     indexed value
     * @param prevTuple index of last tuple
     * @return index of next tuple or cardinality
     */
    public int nextTuple(int value, int prevTuple) {
        // Get start position for indexed values
        int firstPos = keyToPositions.getOrDefault(value, -1);
        // No indexed values?
        if (firstPos < 0) {
            JoinStats.nrUniqueIndexLookups += 1;
            return cardinality;
        }
        // Get number of indexed values
        int nrVals = getEntry(firstPos);
        // Can we return first indexed value?
        int firstTuple = getEntry(firstPos + 1);
        if (firstTuple > prevTuple) {
            return firstTuple;
        }
        // Update index-related statistics
        JoinStats.nrIndexEntries += nrVals;
        if (nrVals == 1) {
            JoinStats.nrUniqueIndexLookups += 1;
        }
        // Restrict search range via binary search
        int lowerBound = firstPos + 1;
        int upperBound = firstPos + nrVals;
        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            if (getEntry(middle) > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }
        // Get next tuple
        for (int pos = lowerBound; pos <= upperBound; ++pos) {
            int next = getEntry(pos);
            if (next > prevTuple) {
                return next;
            }
        }
        // No suitable tuple found
        return cardinality;
    }

    /**
     * Returns index of next tuple with given value
     * or cardinality of indexed table if no such
     * tuple exists.
     *
     * @param value     indexed value
     * @param prevTuple index of last tuple
     * @return index of next tuple or cardinality
     */
    public int nextTupleInPage(int value, int prevTuple) {
        // Get start position for indexed values
        int pageLen = BufferConfig.pageSize / 4;
        int firstPos = keyToPositions.getOrDefault(value, -1) + 1;
        // No indexed values?
        if (firstPos <= 0) {
            JoinStats.nrUniqueIndexLookups += 1;
            return cardinality;
        }
        // Get number of indexed values
        int nrVals = keyToNumber.get(value);
        int[] meta = keyToMeta.getOrDefault(value, null);
        int firstTuple = meta[0];
        if (firstTuple > prevTuple) {
            return firstTuple;
        }
        // Get index of pages where the next tuple locates.
        int index = Arrays.binarySearch(meta, prevTuple + 1);
        if (index >= 0) {
            return prevTuple + 1;
        }
        int pageStart = firstPos / pageLen * pageLen;
        int finalIndex = -1 * index - 2;
        if (finalIndex == meta.length - 1) {
            return cardinality;
        }
        // the index of last satisfied row.
        int pageFirst = pageStart + finalIndex * pageLen;
        // Restrict search range via binary search
        int lowerBound = Math.max(pageFirst + 1, firstPos);
        int upperBound = Math.min(pageFirst + pageLen, firstPos + nrVals - 1);
        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            if (getEntry(middle) > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }
        // Get next tuple
        for (int pos = lowerBound; pos <= upperBound; ++pos) {
            int next = getEntry(pos);
            if (next > prevTuple) {
                return next;
            }
        }
        // No suitable tuple found
        return cardinality;
    }
    /**
     * Returns the number of entries indexed
     * for the given value.
     *
     * @param value count indexed tuples for this value
     * @return number of indexed values
     */
    public int nrIndexed(int value) {
        int firstPos = keyToPositions.getOrDefault(value, -1);
        if (firstPos < 0) {
            return 0;
        } else {
            return getEntry(firstPos);
        }
    }

    /**
     * Initializes iterator over positions
     * at which given value appears.
     *
     * @param value indexed value
     */
    public void initIter(int value) {
        iterPos = keyToPositions.getOrDefault(value, -1);
        if (iterPos != -1) {
            int nrVals = getEntry(iterPos);
            lastIterPos = iterPos + nrVals;
        }
    }

    /**
     * Return next tuple index from current
     * iterator and advance iterator.
     *
     * @return next tuple index or cardinality
     */
    public int iterNext() {
        if (iterPos < 0 || iterPos >= lastIterPos) {
            return cardinality;
        } else {
            ++iterPos;
            return getEntry(iterPos);
        }
    }

    /**
     * Returns next indexed tuple from remaining
     * iterator entries after given prior tuple.
     * Advances iterator until that position.
     *
     * @param prevTuple search tuples with higher tuple index
     * @return tuple index or -1 if no tuple found
     */
    public int iterNextHigher(int prevTuple) {
        // Iterator depleted?
        if (iterPos < 0 || iterPos >= lastIterPos) {
            return cardinality;
        }
        // Otherwise: use binary search
        int lowerBound = iterPos + 1;
        int upperBound = lastIterPos;
        while (upperBound - lowerBound > 1) {
            int middle = lowerBound + (upperBound - lowerBound) / 2;
            if (getEntry(middle) > prevTuple) {
                upperBound = middle;
            } else {
                lowerBound = middle;
            }
        }
        // Get next tuple
        for (int pos = lowerBound; pos <= upperBound; ++pos) {
            int next = getEntry(pos);
            if (next > prevTuple) {
                // Advance iterator and return position
                iterPos = pos;
                return next;
            }
        }
        // No matching tuple found
        iterPos = lastIterPos;
        return cardinality;
    }

    /**
     * Get the entry in given position.
     *
     * @param pos the index of positions
     * @return the according entry.
     */
    public int getEntry(int pos) {
        int returnVal = -1;
        try {
            returnVal = BufferManager.getIndexData(this, pos);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnVal;
    }

    /**
     * Output given log text if activated.
     *
     * @param logText text to log if activated
     */
    void log(String logText) {
        if (LoggingConfig.INDEXING_VERBOSE) {
            System.out.println(logText);
        }
    }

    /**
     * Close the channel that is opened
     * during index generation.
     *
     * @throws IOException
     */
    public void closeChannels() throws IOException {
        // close the channel
        positionChannel.close();
        // remove temporary files
        Path index = Paths.get(PathUtil.indexPath, columnRef.toString() + ".idx");
        Files.deleteIfExists(index);
    }

    @Override
    public void store(int[] positions) throws Exception {
        // store positions data
        Path indexPath = Files.createFile(Paths.get(PathUtil.indexPath, columnRef.toString() + ".idx"));
        Set<StandardOpenOption> options = new HashSet<>();
        options.add(StandardOpenOption.CREATE);
        options.add(StandardOpenOption.READ);
        options.add(StandardOpenOption.WRITE);
        options.add(StandardOpenOption.SYNC);

        positionChannel = Files.newByteChannel(indexPath, options);
        // write data to the int buffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * positions.length);
        byteBuffer.order(ByteOrder.nativeOrder());
        byteBuffer.asIntBuffer().put(positions);
        positionChannel.write(byteBuffer);

    }

    @Override
    public void clear() {
        positions = null;
        loadedStartID.clear();
        pageOrder.clear();
        for (EntryRef entryRef : entryRefs) {
            entryRef.positions = null;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof IntIndex) {
            IntIndex otherIndex = (IntIndex) other;
            return otherIndex.cid == cid;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return cid;
    }
}
