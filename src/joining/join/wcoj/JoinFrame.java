package joining.join.wcoj;

import java.util.List;

/**
 * Summarizes all information about one specific
 * variable in a join order that is relevant to
 * resume LFT join.
 * 
 * @author immanueltrummer
 *
 */
public class JoinFrame {
	/**
	 * Index of variable that this join
	 * frame is associated with.
	 */
	//int curVariableID = -1;
	/**
	 * Relevant LFTJ iterators that are
	 * relevant to current variable.
	 */
	List<LFTJiter> curIters = null;
	/**
	 * Number of currently relevant iterators.
	 */
	int nrCurIters = -1;
	/**
	 * Marks position of next iterator to advance
	 * (typically this iterator has the lowest
	 * key value among all current iterators).
	 */
	int p = -1;
	/**
	 * Marks position of iterator that has currently
	 * the maximal key value among current iterators.
	 */
	int maxIterPos = -1;
	/**
	 * Maximal key value at which any of current
	 * iterators is positioned.
	 */
	int maxKey = -1;
}
