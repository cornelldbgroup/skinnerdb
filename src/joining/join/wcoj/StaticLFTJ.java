package joining.join.wcoj;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import buffer.BufferManager;
import config.CheckConfig;
import data.ColumnData;
import data.IntData;
import joining.join.MultiWayJoin;
import joining.result.JoinResult;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

/**
 * An LFTJ operator with a fixed join order
 * that is chosen at initialization. Allows
 * to resume execution for single time slices.
 * 
 * @author immanueltrummer
 *
 */
public class StaticLFTJ extends MultiWayJoin {
	/**
	 * Maps alias IDs to corresponding iterator.
	 */
	final Map<String, LFTJiter> aliasToIter;
	/**
	 * Contains at i-th position iterator over
	 * i-th element in query from clause.
	 */
	final LFTJiter[] idToIter;
	/**
	 * Order of variables (i.e., equivalence classes
	 * of join attributes connected via equality
	 * predicates).
	 */
	final List<Set<ColumnRef>> varOrder;
	/**
	 * Contains at i-th position the iterators
	 * involved in obtaining keys for i-th
	 * variable (consistent with global
	 * variable order).
	 */
	final List<List<LFTJiter>> itersByVar;
	/**
	 * Number of variables in input query (i.e.,
	 * number of equivalence classes of join columns
	 * connected via equality predicates).
	 */
	final int nrVars;
	/**
	 * Whether entire result was generated.
	 */
	boolean finished = false;
	/**
	 * Counds iterations of the main loop.
	 */
	long roundCtr = 0;
	/**
	 * Bookkeeping information associated 
	 * with attributes (needed to resume join).
	 */
	Stack<JoinFrame> joinStack = new Stack<>();
	/**
	 * Whether we backtracked in the last iteration
	 * of the main loop (requires certain actions
	 * at the beginning of loop).
	 */
	boolean backtracked = false;
	/**
	 * Number of result tuples produced 
	 * in last invocation (used for reward
	 * calculations).
	 */
	public int lastNrResults = -1;
	/**
	 * Percentage of work done for first
	 * attribute in attribute order (used
	 * for reward calculations).
	 */
	public double firstCovered = -1;
	/**
	 * Initialize join for given query.
	 * 
	 * @param query				join query to process via LFTJ
	 * @param executionContext	summarizes procesing context
	 * @throws Exception
	 */
	public StaticLFTJ(QueryInfo query, Context executionContext, 
			JoinResult joinResult) throws Exception {
		// Initialize query and context variables
		super(query, executionContext, joinResult);
		// Choose variable order arbitrarily
		varOrder = new ArrayList<>();
		varOrder.addAll(query.equiJoinClasses);
		nrVars = query.equiJoinClasses.size();
		Collections.shuffle(varOrder);
		System.out.println("Variable Order: " + varOrder);
		// Initialize iterators
		aliasToIter = new HashMap<>();
		idToIter = new LFTJiter[nrJoined];
		for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
			String alias = query.aliases[aliasCtr];
			LFTJiter iter = new LFTJiter(query, 
					executionContext, aliasCtr, varOrder);
			aliasToIter.put(alias, iter);
			idToIter[aliasCtr] = iter;
		}
		// Group iterators by variable
		itersByVar = new ArrayList<>();
		for (Set<ColumnRef> var : varOrder) {
			List<LFTJiter> curVarIters = new ArrayList<>();
			for (ColumnRef colRef : var) {
				String alias = colRef.aliasName;
				LFTJiter iter = aliasToIter.get(alias);
				curVarIters.add(iter);
			}
			itersByVar.add(curVarIters);
		}
		// Initialize stack for LFTJ algorithm
		JoinFrame joinFrame = new JoinFrame();
		joinFrame.curVariableID = 0;
		joinStack.push(joinFrame);
	}
	/**
	 * Initializes iterators and checks for
	 * quick termination.
	 * 
	 * @param iters		iterators for current attribute
	 * @return			true if join continues
	 * @throws Exception
	 */
	boolean leapfrogInit(List<LFTJiter> iters) throws Exception {
		// Advance to next trie level (iterators are
		// initially positioned before first trie level).
		for (LFTJiter iter : iters) {
			iter.open();
		}
		// Check for early termination
		for (LFTJiter iter : iters) {
			if (iter.atEnd()) {
				return false;
			}
		}
		// Sort iterators by their keys
		Collections.sort(iters, new Comparator<LFTJiter>() {
			@Override
			public int compare(LFTJiter o1, LFTJiter o2) {
				return Integer.compare(o1.key(), o2.key());
			}
		});
		// Must continue with join
		return true;
	}
	/**
	 * Add join result tuple based on current
	 * iterator positions.
	 */
	void addResultTuple() throws Exception {
		// Update reward-related statistics
		++lastNrResults;
		// Generate result tuple
		int[] resultTuple = new int[nrJoined];
		// Iterate over all joined tables
		for (int aliasCtr=0; aliasCtr<nrJoined; ++aliasCtr) {
			LFTJiter iter = idToIter[aliasCtr];
			resultTuple[aliasCtr] = iter.rid();
		}
		// Add new result tuple
		result.add(resultTuple);
		// Verify result tuple if activated
		if (CheckConfig.CHECK_LFTJ_RESULTS) {
			if (!testResult(resultTuple)) {
				System.out.println(
						"Error - inconsistent result tuple: "
						+ Arrays.toString(resultTuple));
			}
		}
	}
	/**
	 * Returns true iff given result tuples satisfies
	 * all binary join equality predicates.
	 * 
	 * @param resultTuple	check this result tuple
	 * @return				true iff tuple passes checks
	 * @throws Exception
	 */
	boolean testResult(int[] resultTuple) throws Exception {
		// Iterate over equality join conditions
		for (Set<ColumnRef> equiPair : query.equiJoinPairs) {
			Set<Integer> keyVals = new HashSet<>();
			// Iterate over columns in equality condition
			for (ColumnRef colRef : equiPair) {
				// Retrieve tuple index
				String alias = colRef.aliasName;
				int aliasIdx = query.aliasToIndex.get(alias);
				int tupleIdx = resultTuple[aliasIdx];
				// Retrieve corresponding data
				String table = preSummary.aliasToFiltered.get(alias);
				String column = colRef.columnName;
				ColumnRef baseRef = new ColumnRef(table, column);
				ColumnData data = BufferManager.getData(baseRef);
				IntData intData = (IntData)data;
				int key = intData.data[tupleIdx];
				keyVals.add(key);
			}
			// Check whether key values collapse
			if (keyVals.size()>1) {
				System.out.println(
						"Equality not satisfied: " +
						equiPair.toString());
				return false;
			}
			/*
			else {
				System.out.println(
						"Equality satisfied: " +
						equiPair.toString());
			}
			*/
		}
		// No inconsistencies were found - passed check
		return true;
	}
	/**
	 * Advance to next variable in join order.
	 */
	void advance() {
		JoinFrame oldFrame = joinStack.peek();
		JoinFrame newFrame = new JoinFrame();
		newFrame.curVariableID = oldFrame.curVariableID+1;
		joinStack.push(newFrame);
	}
	/**
	 * Backtrack to previous variable in join order.
	 */
	void backtrack() {
		joinStack.pop();
		backtracked = true;
	}
	/**
	 * Resumes join operation for a fixed number of steps.
	 * 
	 * @param budget		how many iterations are allowed
	 * @throws Exception
	 */
	void resumeJoin(long budget) throws Exception {
		// Initialize reward-related statistics
		lastNrResults = 0;
		// Do we freshly resume after being suspended?
		boolean afterSuspension = roundCtr>0;
		// Until we finish processing (break)
		while (true) {
			// Did we finish processing?
			if (joinStack.empty()) {
				finished = true;
				break;
			}
			JoinFrame joinFrame = joinStack.peek();
			// Go directly to point of interrupt?
			if (afterSuspension) {
				afterSuspension = false;
			} else {
				if (backtracked) {
					backtracked = false;
					LFTJiter minIter = joinFrame.curIters.get(joinFrame.p);
					minIter.seek(joinFrame.maxKey+1);
					if (minIter.atEnd()) {
						// Go one level up in each trie
						for (LFTJiter iter : joinFrame.curIters) {
							iter.up();
						}
						backtrack();
						continue;
					}
					joinFrame.maxKey = minIter.key();
					joinFrame.p = (joinFrame.p + 1) % joinFrame.nrCurIters;
				} else {
					// Have we completed a result tuple?
					if (joinFrame.curVariableID >= nrVars) {
						addResultTuple();
						backtrack();
						continue;
					}
					// Collect relevant iterators
					joinFrame.curIters = itersByVar.get(joinFrame.curVariableID);
					joinFrame.nrCurIters = joinFrame.curIters.size();
					// Order iterators and check for early termination
					if(!leapfrogInit(joinFrame.curIters)) {
						// Go one level up in each trie
						for (LFTJiter iter : joinFrame.curIters) {
							iter.up();
						}
						backtrack();
						continue;
					}
					// Execute search procedure
					joinFrame.p = 0;
					joinFrame.maxIterPos = (joinFrame.nrCurIters+joinFrame.p-1) % joinFrame.nrCurIters;
					joinFrame.maxKey = joinFrame.curIters.get(joinFrame.maxIterPos).key();				
				}
			}
			while (true) {
				// Count current round
				++roundCtr;
				--budget;
				JoinStats.nrIterations++;
				// Get current key
				LFTJiter minIter = joinFrame.curIters.get(joinFrame.p);
				int minKey = minIter.key();
				// Generate debugging output
				if (roundCtr < 10) {
					System.out.println("--- Current variable ID: " + joinFrame.curVariableID);
					System.out.println("p: " + joinFrame.p);
					System.out.println("minKey: " + minKey);
					System.out.println("maxKey: " + joinFrame.maxKey);
					for (LFTJiter iter : joinFrame.curIters) {
						System.out.println(iter.rid() + ":" + iter.key());
					}
				}
				// Did we find a match between iterators?
				if (minKey == joinFrame.maxKey) {
					advance();
					break;
				} else {
					minIter.seek(joinFrame.maxKey);
					if (minIter.atEnd()) {
						// Go one level up in each trie
						for (LFTJiter iter : joinFrame.curIters) {
							iter.up();
						}
						backtrack();
						break;
					} else {
						// Min-iter to max-iter
						joinFrame.maxKey = minIter.key();
						joinFrame.p = (joinFrame.p + 1) % joinFrame.nrCurIters;
					}
				}
			}
		}
	}
	@Override
	public boolean isFinished() {
		return finished;
	}
}
