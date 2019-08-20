package indexing;

import buffer.BufferManager;
import buffer.EntryRef;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import com.sun.tools.javah.Gen;
import config.GeneralConfig;
import config.LoggingConfig;
import data.IntData;
import diskio.PathUtil;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
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
 *
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
	 * Number of position information
	 */
	public int prefixSum;
	/**
	 * After indexing: maps search key to index
	 * of first position at which associated
	 * information is stored.
	 */
	public IntIntMap keyToPositions;
    public IntIntMap keyToPageSize;
	/**
	 * Local memory saving a mapping from
	 * distinct value to part of positions
	 * that is encapsulated in EntryRef.
	 */
	public Map<Integer, EntryRef> keyToEntries;
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
	 * @param colRef	create index on this column
	 * @param intData	integer data to index
	 */
	public IntIndex(ColumnRef colRef, IntData intData) {
		long startMillis = System.currentTimeMillis();
		// Extract info
		this.intData = intData;
		this.cardinality = intData.cardinality;
		this.columnRef = colRef;
		this.cid = BufferManager.colToID.get(colRef);
		// load data from the disk
		Path indexPath = Paths.get(PathUtil.indexPath, columnRef.toString() + ".idx");
		if (Files.exists(indexPath) && !GeneralConfig.indexInMemory) {
			// load keys map
			int[] data = intData.data;
			// Count number of occurrences for each value
			IntIntMap keyToNr = HashIntIntMaps.newMutableMap();
			for (int i=0; i<cardinality; ++i) {
				// Don't index null values
				if (!intData.isNull.get(i)) {
					int value = data[i];
					int nr = keyToNr.getOrDefault(value, 0);
					keyToNr.put(value, nr+1);
				}
			}
			// Assign each key to the appropriate position offset
			int nrKeys = keyToNr.size();
			log("Number of keys:\t" + nrKeys);
			keyToPositions = HashIntIntMaps.newMutableMap(nrKeys);
			keyToPageSize = HashIntIntMaps.newMutableMap(nrKeys);
			keyToEntries = new HashMap<>();
			int prefixSum = 0;
			IntIntCursor keyToNrCursor = keyToNr.cursor();
			while (keyToNrCursor.moveNext()) {
				int key = keyToNrCursor.key();
				keyToPositions.put(key, prefixSum);
				// Advance offset taking into account
				// space for row indices and one field
				// storing the number of following indices.
				int nrFields = keyToNrCursor.value() + 1;
                keyToPageSize.put(key, nrFields);
				prefixSum += nrFields;
			}
			this.prefixSum = prefixSum;

			// load positions channel
			Set<StandardOpenOption> options = new HashSet<>();
			options.add(StandardOpenOption.READ);
			options.add(StandardOpenOption.WRITE);
			try {
				positionChannel = Files.newByteChannel(indexPath, options);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			int[] data = intData.data;
			// Count number of occurrences for each value
			IntIntMap keyToNr = HashIntIntMaps.newMutableMap();
			for (int i=0; i<cardinality; ++i) {
				// Don't index null values
				if (!intData.isNull.get(i)) {
					int value = data[i];
					int nr = keyToNr.getOrDefault(value, 0);
					keyToNr.put(value, nr+1);
				}
			}
			// Assign each key to the appropriate position offset
			int nrKeys = keyToNr.size();
			log("Number of keys:\t" + nrKeys);
			keyToPositions = HashIntIntMaps.newMutableMap(nrKeys);
            keyToPageSize = HashIntIntMaps.newMutableMap(nrKeys);
			keyToEntries = new HashMap<>();
			int prefixSum = 0;
			IntIntCursor keyToNrCursor = keyToNr.cursor();
			while (keyToNrCursor.moveNext()) {
				int key = keyToNrCursor.key();
				keyToPositions.put(key, prefixSum);
				// Advance offset taking into account
				// space for row indices and one field
				// storing the number of following indices.
				int nrFields = keyToNrCursor.value() + 1;
                keyToPageSize.put(key, nrFields);
				prefixSum += nrFields;
			}
			log("Prefix sum:\t" + prefixSum);
			// Generate position information
			int[] positions = new int[prefixSum];
			for (int i=0; i<cardinality; ++i) {
				if (!intData.isNull.get(i)) {
					int key = data[i];
					int startPos = keyToPositions.get(key);
					positions[startPos] += 1;
					int offset = positions[startPos];
					int pos = startPos + offset;
					positions[pos] = i;
				}
			}
			this.prefixSum = positions.length;

			// Output statistics for performance tuning
			if (LoggingConfig.INDEXING_VERBOSE) {
				long totalMillis = System.currentTimeMillis() - startMillis;
				log("Created index for integer column with cardinality " +
						cardinality + " in " + totalMillis + " ms.");
			}
			// Check index if enabled
			IndexChecker.checkIndex(intData, this);
			long startWriteMillis = System.currentTimeMillis();
			// write indexes to files when the inMemory is not enable.
			if (!GeneralConfig.indexInMemory) {
				try {
					store(positions);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else {
				this.positions = positions;
			}
		}

		long end = System.currentTimeMillis();
//		System.out.println(colRef.toString() + ": " + (end - startMillis) + " ms");
	}
	/**
	 * Returns index of next tuple with given value
	 * or cardinality of indexed table if no such
	 * tuple exists.
	 * 
	 * @param value			indexed value
	 * @param prevTuple		index of last tuple
	 * @return 	index of next tuple or cardinality
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
		int firstTuple = getEntry(firstPos+1);
		if (firstTuple>prevTuple) {
			return firstTuple;
		}
		// Update index-related statistics
		JoinStats.nrIndexEntries += nrVals;
		if (nrVals==1) {
			JoinStats.nrUniqueIndexLookups += 1;
		}
		// Restrict search range via binary search
		int lowerBound = firstPos + 1;
		int upperBound = firstPos + nrVals;
		while (upperBound-lowerBound>1) {
			int middle = lowerBound + (upperBound-lowerBound)/2;
			if (getEntry(middle) > prevTuple) {
				upperBound = middle;
			} else {
				lowerBound = middle;
			}
		}
		// Get next tuple
		for (int pos=lowerBound; pos<=upperBound; ++pos) {
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
	 * @param value	count indexed tuples for this value
	 * @return		number of indexed values
	 */
	public int nrIndexed(int value) {
		int firstPos = keyToPositions.getOrDefault(value, -1);
		if (firstPos<0) {
			return 0;
		} else {
			return getEntry(firstPos);
		}
	}
	/**
	 * Initializes iterator over positions
	 * at which given value appears.
	 * 
	 * @param value	indexed value
	 */
	public void initIter(int value) {
		iterPos = keyToPositions.getOrDefault(value, -1);
		if (iterPos!=-1) {
			int nrVals = getEntry(iterPos);
			lastIterPos = iterPos + nrVals;
		}
	}
	/**
	 * Return next tuple index from current
	 * iterator and advance iterator.
	 * 
	 * @return	next tuple index or cardinality
	 */
	public int iterNext() {
		if (iterPos<0 || iterPos>=lastIterPos) {
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
	 * @param prevTuple	search tuples with higher tuple index
	 * @return			tuple index or -1 if no tuple found
	 */
	public int iterNextHigher(int prevTuple) {
		// Iterator depleted?
		if (iterPos<0 || iterPos>=lastIterPos) {
			return cardinality;
		}
		// Otherwise: use binary search
		int lowerBound = iterPos+1;
		int upperBound = lastIterPos;
		while (upperBound-lowerBound>1) {
			int middle = lowerBound + (upperBound-lowerBound)/2;
			if (getEntry(middle) > prevTuple) {
				upperBound = middle;
			} else {
				lowerBound = middle;
			}
		}
		// Get next tuple
		for (int pos=lowerBound; pos<=upperBound; ++pos) {
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
	 * @param pos	the index of positions
	 * @return 		the according entry.
	 */
	public int getEntry(int pos) {
		if (GeneralConfig.indexInMemory) {
			return positions[pos];
		}
		else {
			try {
				return BufferManager.getIndexData(this, pos);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return positions[pos];
	}

	/**
	 * Output given log text if activated.
	 * 
	 * @param logText	text to log if activated
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
		Path index = Paths.get(PathUtil.indexPath,columnRef.toString() + ".idx");
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
		positionChannel = Files.newByteChannel(indexPath, options);
		// write data to the int buffer
		ByteBuffer byteBuffer = ByteBuffer.allocate(4 * positions.length);
		byteBuffer.order(ByteOrder.nativeOrder());
		byteBuffer.asIntBuffer().put(positions);
		positionChannel.write(byteBuffer);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof IntIndex) {
			IntIndex otherIndex = (IntIndex)other;
			return otherIndex.cid == cid;
		} else {
			return false;
		}
	}
}
