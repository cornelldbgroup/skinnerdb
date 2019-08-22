package buffer;

import data.ColumnData;
import indexing.IntIndex;
import query.ColumnRef;

public interface IDataManager {
    public ColumnData getData(ColumnRef columnRef) throws Exception;
    /**
     * Get the value of entry given a index of positions.
     * If the positions array is in the memory, return positions[pos] directly.
     * Otherwise, load the entire array from the disk.
     *
     * @param intIndex  column index.
     * @param pos       index in positions array.
     * @return          the value located in positions array.
     * @throws Exception
     */
    public int getIndexData(IntIndex intIndex, int pos) throws Exception;

    /**
     * Get the value of entry in a window.
     * If the positions window is in the memory, return positions[pos] directly.
     * Otherwise, load the window from the disk.
     *
     * @param intIndex  column index.
     * @param pos       index in positions array.
     * @return
     * @throws Exception
     */
    public int getDataInWindow(IntIndex intIndex, int pos) throws Exception;
    /**
     * clear data that is saved in the cache
     */
    public void clearCache();
    /**
     * Log given text if buffer logging activated.
     *
     * @param text	text to output
     */
    public void log(String text);

    public void unloadIndex(IntIndex intIndex);
}
