package joining.join.wcoj;

import java.util.ArrayList;
import java.util.List;

import joining.join.DynamicMWJoin;
import preprocessing.Context;
import query.QueryInfo;

/**
 * Implements variant of the Leapfrog Trie Join
 * (see paper "Leapfrog Triejoin: a worst-case
 * optimal join algorithm" by T. Veldhuizen).
 * 
 * @author immanueltrummer
 */
public class DynamicLFTJ extends DynamicMWJoin {
	/**
	 * How many attribute orders to
	 * choose from via learning.
	 */
	final int nrOrders = 3;
	/**
	 * All available attribute orders.
	 */
	List<StaticLFTJ> staticOrderOps = new ArrayList<>();
	/**
	 * Milliseconds at which joins start.
	 */
	long joinStartMillis = -1;
	
	public DynamicLFTJ(QueryInfo query, 
			Context executionContext) throws Exception {
		super(query, executionContext);
		// Prepare join with different attribute orders
		long startMillis = System.currentTimeMillis();
		for (int orderCtr=0; orderCtr<nrOrders; ++orderCtr) {
			staticOrderOps.add(new StaticLFTJ(
					query, executionContext, this.result));
		}
		long totalMillis = System.currentTimeMillis() - startMillis;
		System.out.println("Preparation took " + totalMillis + " ms");
		joinStartMillis = System.currentTimeMillis();
	}

	@Override
	public double execute(int[] order) throws Exception {
		int pick = order[0] % nrOrders;
		StaticLFTJ pickedOp = staticOrderOps.get(pick);
		pickedOp.resumeJoin(500);
		return pickedOp.lastNrResults;
	}

	@Override
	public boolean isFinished() {
		// Check for timeout
		if (System.currentTimeMillis() - joinStartMillis > 60000) {
			return true;
		}
		// Check whether full result generated
		for (StaticLFTJ staticOp : staticOrderOps) {
			if (staticOp.isFinished()) {
				return true;
			}
		}
		return false;
	}
	
}
