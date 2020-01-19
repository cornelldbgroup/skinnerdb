package indexing;

/**
 * A placeholder used in situations where an index is
 * expected but none available. Simply proposes to 
 * look at the next tuple for the given search key.
 * 
 * @author immanueltrummer
 *
 */
public class DummyIntIndex extends IntIndex {
	/**
	 *	Initialize for default cardinality. 
	 */
	public DummyIntIndex() {
		super(0);
	}
	/**
	 * Simply propose to look at following tuple
	 * (not guaranteed to have search key).
	 */
	@Override
	public int nextTuple(int value, int prevTuple) {
		return prevTuple+1;
	}
	/**
	 * Return default value.
	 */
	@Override
	public int nrIndexed(int value) {
		return -1;
	}
}
