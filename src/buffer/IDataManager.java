package buffer;

import data.ColumnData;
import indexing.IntIndex;
import query.ColumnRef;

public interface IDataManager {
    public ColumnData getData(ColumnRef columnRef) throws Exception;
    public int getIndexData(IntIndex intIndex, int pos) throws Exception;
}
