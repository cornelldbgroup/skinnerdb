package joining.join.wcoj;

import joining.join.MultiWayJoin;
import query.QueryInfo;

/**
 * Implements variant of the Leapfrog Trie Join.
 */
public class LFTjoin extends MultiWayJoin {

	public LFTjoin(QueryInfo query) throws Exception {
		super(query);
	}

	@Override
	public double execute(int[] order) throws Exception {
		
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isFinished() {
		// The entire join is currently executed in a single
		// time slice - immediate termination after first
		// invocation of execution method.
		return true;
	}

}
