package buffer;

import data.ColumnData;
import query.ColumnRef;

public interface IDataManager {
    public ColumnData getData(ColumnRef columnRef) throws Exception;
}
