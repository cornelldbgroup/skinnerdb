package indexing;

/**
 * Common superclass of all indexing structures.
 * 
 * @author immanueltrummer
 *
 */
public abstract class Index {
    public abstract int nextTuple(int value, int prevTuple);
    public abstract int nextTupleInScope(int value, int prevTuple, int tid);
    public abstract int nrIndexed(int value);
    public abstract void initIter(int value);
    public abstract int iterNextHigher(int prevTuple);
    public abstract int iterNext();
    public abstract boolean evaluate(int priorVal, int curIndex, int splitTable, int nextTable, int tid);
}
