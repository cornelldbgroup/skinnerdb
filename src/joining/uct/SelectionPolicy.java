package joining.uct;

/**
 * Describes the policy according to which
 * actions (i.e., tables to join) are
 * selected for each search tree node.
 * 
 * @author immanueltrummer
 *
 */
public enum SelectionPolicy {
	UCB1,			// apply the standard UCT formula 
	MAX_REWARD,		// use actions with maximal reward
	EPSILON_GREEDY,	// apply epsilon-greedy selection
	RANDOM,			// select with uniform random distribution
	RANDOM_UCB1		// random selection in root, then UCB1
}
