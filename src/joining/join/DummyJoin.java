package joining.join;

import java.util.Random;

import query.QueryInfo;

/**
 * This class is used just for testing purposes
 * and returns random reward for given join orders.
 * 
 * @author immanueltrummer
 *
 */
public class DummyJoin extends DynamicMWJoin {
	/**
	 * Random generator used for rewards.
	 */
	Random random = new Random();

	public DummyJoin(QueryInfo query) throws Exception {
		// FIXME: This will throw a null pointer exception
		super(query, null);
	}

	@Override
	public double execute(int[] order) throws Exception {
		return random.nextDouble();
	}

	@Override
	public boolean isFinished() {
		return false;
	}

}
