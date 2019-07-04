package config;

import joining.uct.ExplorationWeightPolicy;
import joining.uct.SelectionPolicy;

/**
 * Configures reinforcement-learning based
 * search for optimal join orders.
 * 
 * @author immanueltrummer
 *
 */
public class JoinConfig {
	/**
	 * Choose default action selection strategy.
	 */
	public static final SelectionPolicy DEFAULT_SELECTION =
			SelectionPolicy.UCB1;
	/**
	 * Number of steps performed per episode.
	 */
	public static int BUDGET_PER_EPISODE = 500;
	/**
	 * Weight for UCT exploration term (used to select
	 * most interesting action to try next). This
	 * factor may be dynamically adapted.
	 */
	public static double EXPLORATION_WEIGHT = 1E-5;
	/**
	 * Determines how the weight for the exploration term
	 * of the UCT algorithm is updated over time.
	 */
	public static final ExplorationWeightPolicy EXPLORATION_POLICY =
			ExplorationWeightPolicy.STATIC;
	/**
	 * The epsilon term used for epsilon greedy selection
	 * (i.e., the probability that a random action is
	 * selected as opposed to the maximum reward action).
	 */
	public static final double EPSILON = 0.1;
	/**
	 * Whether to regularly forget everything that was
	 * learned about join orders so far. This is helpful
	 * since the reward distribution keeps changing due
	 * to table offsets etc. Also helps against 
	 * non-uniform data which may cause (too) early
	 * convergence to one specific join order.
	 */
	public static final boolean FORGET = true;
}
