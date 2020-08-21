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

import buffer.BufferManager;
import config.CheckConfig;
import data.ColumnData;
import data.IntData;
import joining.join.MultiWayJoin;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import statistics.JoinStats;

/**
 * Implements variant of the Leapfrog Trie Join
 * (see paper "Leapfrog Triejoin: a worst-case
 * optimal join algorithm" by T. Veldhuizen).
 */
public class LFTjoin extends MultiWayJoin {
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
	 * Initialize join for given query.
	 * 
	 * @param query			join query to process via LFTJ
	 * @param preSummary	summarizes effects of pre-processing
	 * @throws Exception
	 */
	public LFTjoin(QueryInfo query, Context preSummary) throws Exception {
		super(query, preSummary);		
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
					preSummary, aliasCtr, varOrder);
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
		//System.out.println("addResultTuple");
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
	
	long roundCtr = 0;
	
	/**
	 * Execute leapfrog trie join for given variable.
	 * 
	 * @param curVariableID	variable index in global variable order
	 * @throws Exception
	 */
	void executeLFTJ(int curVariableID) throws Exception {
		// Check for timeout
		if (System.currentTimeMillis() - startMillis > 60000) {
			return;
		}
		// Have we completed a result tuple?
		if (curVariableID >= nrVars) {
			addResultTuple();
			return;
		}
		// Collect relevant iterators
		List<LFTJiter> curIters = itersByVar.get(curVariableID);
		int nrCurIters = curIters.size();
		// Order iterators and check for early termination
		if(!leapfrogInit(curIters)) {
			// Go one level up in each trie
			for (LFTJiter iter : curIters) {
				iter.up();
			}
			return;
		}
		// Execute search procedure
		int p = 0;
		int maxIterPos = (nrCurIters+p-1) % nrCurIters;
		int maxKey = curIters.get(maxIterPos).key();
		while (true) {
			// Update statistics
			JoinStats.nrIterations++;
			// Get current key
			LFTJiter minIter = curIters.get(p);
			int minKey = minIter.key();
			// Generate debugging output
			++roundCtr;
			if (roundCtr < 10) {
				System.out.println("--- Current variable ID: " + curVariableID);
				System.out.println("p: " + p);
				System.out.println("minKey: " + minKey);
				System.out.println("maxKey: " + maxKey);
				for (LFTJiter iter : curIters) {
					System.out.println(iter.rid() + ":" + iter.key());
				}
			}
			// Did we find a match between iterators?
			if (minKey == maxKey) {
				executeLFTJ(curVariableID+1);
				minIter.seek(maxKey+1);
				if (minIter.atEnd()) {
					// Go one level up in each trie
					for (LFTJiter iter : curIters) {
						iter.up();
					}
					return;
				}
				maxKey = minIter.key();
				p = (p + 1) % nrCurIters;
			} else {
				minIter.seek(maxKey);
				if (minIter.atEnd()) {
					// Go one level up in each trie
					for (LFTJiter iter : curIters) {
						iter.up();
					}
					return;
				} else {
					// Min-iter to max-iter
					maxKey = minIter.key();
					p = (p + 1) % nrCurIters;
				}
			}
		}
	}

	long startMillis = -1;
	
	@Override
	public double execute(int[] order) throws Exception {
		// Retrieve result via WCOJ
		startMillis = System.currentTimeMillis();
		executeLFTJ(0);
		// Set termination flag
		finished = true;
		// Return dummy reward
		return 1;
	}

	@Override
	public boolean isFinished() {
		return finished;
	}

}
