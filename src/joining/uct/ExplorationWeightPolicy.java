package joining.uct;

/**
 * Defines how the weight for the exploration term
 * in the UCT algorithm is updated over time.
 * 
 * @author immanueltrummer
 *
 */
public enum ExplorationWeightPolicy {
	STATIC,			// no updates to exploration weight
	REWARD_AVERAGE,	// update weight based on reward average
	SCALE_DOWN,		// scale down exploration factor over time
	ADAPT_TO_SAMPLE	// choose weight based on initial reward sample
}
