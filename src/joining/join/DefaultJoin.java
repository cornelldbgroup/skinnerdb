package joining.join;

import joining.plan.JoinOrder;
import joining.plan.LeftDeepPlan;
import joining.progress.ProgressTracker;
import joining.progress.State;
import preprocessing.PreSummary;
import query.QueryInfo;
import statistics.JoinStats;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import config.LoggingConfig;
import expressions.compilation.KnaryBoolEval;

/**
 * Executes query with given join order for a specified
 * number of steps.
 *
 * @author immanueltrummer
 */
public class DefaultJoin extends MultiWayJoin {
    /**
     * Number of steps per episode.
     */
    public final int budget;
    /**
     * Re-initialized in each invocation:
     * stores the remaining budget for
     * the current iteration.
     */
    public int remainingBudget;
    /**
     * Avoids redundant planning work by storing left deep plans.
     */
    final Map<JoinOrder, LeftDeepPlan> planCache;
    /**
     * Avoids redundant evaluation work by tracking evaluation progress.
     */
    public final ProgressTracker tracker;
    /**
     * Contains after each invocation the delta of the tuple
     * indices when comparing start state and final state.
     */
    public final int[] tupleIndexDelta;
    /**
     * Counts number of log entries made.
     */
    int logCtr = 0;
    /**
     * Initializes join algorithm for given input query.
     * 
     * @param query			query to process
     * @param preSummary	summary of pre-processing
     * @param budget		budget per episode
     */
    public DefaultJoin(QueryInfo query, PreSummary preSummary, 
    		int budget) throws Exception {
        super(query, preSummary);
        this.budget = budget;
        this.planCache = new HashMap<>();
        this.tracker = new ProgressTracker(nrJoined, cardinalities);
        this.tupleIndexDelta = new int[nrJoined];
        // Logging
        log("preSummary before join: " + preSummary.toString());
    }
    /**
     * Advances to next tuple by updating specified
     * tuple indices and (potentially) join index.
     * 
     * @param plan			current left-deep plan
     * @param tupleIndices	tuple index for each base table
     * @param joinIndex		current position in join order
     * @return	updated join index
     */
    /*
    int nextTuple(LeftDeepPlan plan, int[] tupleIndices, int joinIndex) {
    	boolean backtrack = false;
    	do {
    		backtrack = false;
    		// Get current table
    		int curTable = plan.joinOrder.order[joinIndex];
    		// Integrate offsets
    		int curTuple = tupleIndices[curTable];
    		int curOffset = tracker.tableOffset[curTable];
    		tupleIndices[curTable] = Math.max(curTuple, curOffset);
    		// Determine tentative next tuple
    		JoinIndexWrapper curIndex = plan.joinIndices[joinIndex];
    		int nextTuple = curIndex==null?
    				tupleIndices[curTable]+1:
    					curIndex.nextIndex(tupleIndices);
    		// No suitable tuple in current table?
    		if (nextTuple < 0 || nextTuple >= cardinalities[curTable]) {
    			tupleIndices[curTable] = -1;
    			--joinIndex;
    			backtrack = true;
    		} else {
    			tupleIndices[curTable] = nextTuple;
    		}
    	} while (backtrack && joinIndex >= 0);
    	return joinIndex;
    }
    */
    /**
     * Calculates reward for progress during one invocation.
     * 
     * @param joinOrder			join order followed
     * @param tupleIndexDelta	difference in tuple indices
     * @param tableOffsets		table offsets (number of tuples fully processed)
     * @return					reward between 0 and 1, proportional to progress
     */
	double reward(int[] joinOrder, int[] tupleIndexDelta, int[] tableOffsets) {
		double progress = 0;
		double weight = 1;
		for (int pos=0; pos<nrJoined; ++pos) {
			// Scale down weight by cardinality of current table
			int curTable = joinOrder[pos];
			int remainingCard = cardinalities[curTable] - 
					(tableOffsets[curTable]+1);
			//int remainingCard = cardinalities[curTable];
			weight *= 1.0 / remainingCard;
			// Fully processed tuples from this table
			progress += tupleIndexDelta[curTable] * weight;
		}
		return progress;
	}
	/**
	 * Calculate next interesting tuple index by intersecting
	 * admissible tuples proposed by each of the index wrappers.
	 * 
	 * @param joinIndices		list of index wrappers
	 * @param curTable			calculate tuple for this table
	 * @param curCardinality	cardinality of current table
	 * @param tupleIndices		vector of base table tuple indices
	 * @return
	 */
	int nextIndex(List<JoinIndexWrapper> joinIndices, int curTable, 
			int curCardinality, int[] tupleIndices) 
	throws Exception {
		int min, max;
		do {
			min = Integer.MAX_VALUE;
			max = Integer.MIN_VALUE;
			for (JoinIndexWrapper wrapper : joinIndices) {
				++JoinStats.nrIndexLookups;
				int next = wrapper.nextIndex(tupleIndices);
				min = Math.min(next, min);
				max = Math.max(next, max);
			}
			if (min<max) {
				tupleIndices[curTable] = max-1;
			}
			// TODO: benchmark this!
			//--remainingBudget;
		} while (min>=0 && min<max);
		if (min == max) {
			return min;
		} else {
			return curCardinality;
		}
	}
	/**
	 * Evaluates list of given predicates on current tuple
	 * indices and returns true iff all predicates evaluate
	 * to true.
	 * 
	 * @param preds				predicates to evaluate
	 * @param tupleIndices		(partial) tuples
	 * @return					true iff all predicates evaluate to true
	 */
	boolean evaluateAll(List<KnaryBoolEval> preds, int[] tupleIndices) {
		for (KnaryBoolEval pred : preds) {
			if (pred.evaluate(tupleIndices)<=0) {
				return false;
			}
		}
		return true;
	}
    /**
     * Executes a given join order for a given budget of steps
     * (i.e., predicate evaluations). Result tuples are added
     * to result set. Budget and result set are created during
     * the class initialization.
     *
     * @param order   table join order
     */
    @Override
    public double execute(int[] order) throws Exception {
    	log("Context:\t" + preSummary.toString());
    	log("Join order:\t" + Arrays.toString(order));
    	log("Aliases:\t" + Arrays.toString(query.aliases));
    	log("Cardinalities:\t" + Arrays.toString(cardinalities));
    	// Lookup or generate left-deep query plan
        JoinOrder joinOrder = new JoinOrder(order);
        LeftDeepPlan plan = planCache.get(joinOrder);
        if (plan == null) {
            plan = new LeftDeepPlan(query, preSummary, predToEval, order);
            planCache.put(joinOrder, plan);
        }
        log(plan.toString());
        // Execute from starting state, save progress, return progress
        State state = tracker.continueFrom(joinOrder);
        int[] offsets = tracker.tableOffset;
        // Extract variables for convenient access
        int[] tupleIndices = Arrays.copyOf(state.tupleIndices, nrJoined);
        int joinIndex = state.lastIndex;
        JoinMove move = state.lastMove;
        // Initialize reward
        double reward = 0;
        double[] tableWeights = new double[nrJoined];
        double curWeight = 1.0;
        for (int pos=0; pos<nrJoined; ++pos) {
        	int curTable = order[pos];
        	int curCard = cardinalities[curTable];
        	curWeight /= (double)curCard;
        	tableWeights[curTable] = curWeight;
        }
        // Counter for number of result tuples produced
        int nrResultTuples = 0;
        // Initialize state and flags to prepare budgeted execution
        remainingBudget = budget;
        // Execute join order until budget depleted or all input finished -
        // at each iteration start, tuple indices contain next tuple
        // combination to look at.
        while (remainingBudget > 0 && joinIndex >= 0) {
        	// Update statistics
        	++JoinStats.nrIterations;
        	// Produce logging output
        	/*
        	log("Join index:\t" + joinIndex + 
        			"; tuples:\t" + Arrays.toString(tupleIndices) +
        			"; move:\t" + move);
        	*/
        	// Decide next join move
        	switch (move) {
        	case RIGHT:
        		// Have complete result tuple
        		if (joinIndex==nrJoined) {
        			++nrResultTuples;
                    result.add(tupleIndices);
                    move = JoinMove.LEFT;
        		} else {
        			move = JoinMove.UP;
        		}
        		break;
        	case LEFT:
        		move = JoinMove.DOWN;
        		break;
        	case UP:
        		move = JoinMove.DOWN;
        		break;
        	case DOWN:
        		int curTable = order[joinIndex];
        		int curTuple = tupleIndices[curTable];
        		int curCard = cardinalities[curTable];
        		if (curTuple>=curCard) {
        			move = JoinMove.LEFT;
        		} else {
            		// Check whether all newly applicable predicates are satisfied
            		List<KnaryBoolEval> evals = plan.applicablePreds.get(joinIndex);
            		if (evaluateAll(evals, tupleIndices)) {
            			move = JoinMove.RIGHT;
            		} else {
            			move = JoinMove.DOWN;
            		}
        		}
        		break;
        	}
        	// Execute chosen move (in terms of join index, 
        	// tuple indices, and budget).
        	switch (move) {
        	case LEFT:
        	{
        		// Update reward
        		/*
        		if (joinIndex<nrJoined) {
            		int curTable = order[joinIndex];
            		int curOffset = offsets[curTable];
            		int curCard = cardinalities[curTable];
            		reward += (curOffset+1-curCard) * tableWeights[curTable];        			
        		}
        		*/
        		// Update join index
        		--joinIndex;
        		// Account for reward
        		if (joinIndex>=0) {
        			int curTable = order[joinIndex];
        			reward -= tableWeights[curTable];
        		}
        		/*
        		if (joinIndex>=0) {
            		int curTable = order[joinIndex];
            		reward += tableWeights[curTable];        			
        		}
        		*/
        		/*
        		if (joinIndex>0) {
            		int curTable = order[joinIndex];
            		int curOffset = offsets[curTable];
            		int curStart = state.tupleIndices[curTable];
            		int curTuple = tupleIndices[curTable];
            		int curFirst = Math.max(curOffset, curStart);
            		curWeight = tableWeights[curTable];
            		double curReward = curWeight * (curTuple - curFirst);
            		reward = Math.max(reward, curReward);        			
        		}
        		*/
        	}
        		break;
        	case RIGHT:
        		++joinIndex;
        		++JoinStats.nrTuples;
        		--remainingBudget;
        		break;
        	case UP:
        	{
        		int curTable = order[joinIndex];
        		tupleIndices[curTable] = -1;
        		/*
        		// Update reward
        		//int curTuple = tupleIndices[curTable];
        		int curCard = cardinalities[curTable];
        		int curOffset = offsets[curTable];
        		// Update tuple indices
        		tupleIndices[curTable] = curOffset;
        		*/
        		// Update reward calculation
        		/*
        		int curCard = cardinalities[curTable];
        		reward -= curCard * tableWeights[curTable];
        		*/
        	}
        		break;
        	case DOWN:
        	{
        		int curTable = order[joinIndex];
        		int curTuple = tupleIndices[curTable];
        		int firstCovered = Math.max(0, curTuple);
        		int curCard = cardinalities[curTable];
        		int curOffset = offsets[curTable];
        		tupleIndices[curTable] = Math.max(tupleIndices[curTable], curOffset);
        		List<JoinIndexWrapper> curIndices = plan.joinIndices.get(joinIndex);
        		if (curIndices.isEmpty()) {
        			++tupleIndices[curTable];
        		} else {
        			/*
        			int max = -1;
        			for (JoinIndexWrapper curIndex : curIndices) {
        				max = Math.max(max, curIndex.nextIndex(tupleIndices));
        			}
        			tupleIndices[curTable] = max;
        			*/
        			tupleIndices[curTable] = nextIndex(curIndices, 
        					curTable, curCard, tupleIndices);
        		}
        		// Update reward calculation
        		int lastCovered = tupleIndices[curTable]-1;
        		int nrCovered = Math.max(0, lastCovered - firstCovered + 1);
        		reward += nrCovered * tableWeights[curTable];
        		/*
        		int curTable = order[joinIndex];
        		List<JoinIndexWrapper> curIndices = plan.joinIndices.get(joinIndex);
        		tupleIndices[curTable] = nextIndex(curIndices, curTable, tupleIndices);
        		*/
        		/*
        		// Determine most selective index
        		JoinIndexWrapper minSelIndex = null;
        		int minNrIndexed = Integer.MAX_VALUE;
        		List<JoinIndexWrapper> curIndices = plan.joinIndices.get(joinIndex);
        		for (JoinIndexWrapper curIndex : curIndices) {
        			int nrIndexed = curIndex.nrIndexed(tupleIndices);
        			if (nrIndexed<minNrIndexed) {
        				minSelIndex = curIndex;
        				minNrIndexed = nrIndexed;
        			}
        		}
        		// Use most selective index for generating next value
        		int curTable = order[joinIndex];
        		if (minSelIndex==null) {
        			++tupleIndices[curTable];
        		} else {
        			tupleIndices[curTable] = minSelIndex.nextIndex(tupleIndices);
        		}
        		*/
        		/*
        		int curTable = order[joinIndex];
        		List<JoinIndexWrapper> curIndices = plan.joinIndices.get(joinIndex);
        		int max = tupleIndices[curTable]+1;
        		for (JoinIndexWrapper curIndex : curIndices) {
        			max = Math.max(curIndex.nextIndex(tupleIndices),max);
        		}
        		tupleIndices[curTable] = max;
        		*/
        	}
        		break;
        	}
        }
        log("Final join index: " + joinIndex);
        // Store tuple index deltas used to calculate reward
        /*
        double reward = 0;
        double curWeight = 1.0;
        for (int pos=0; pos<joinIndex-1; ++pos) {
        	int curTable = order[pos];
        	curWeight /= (double)cardinalities[curTable];
            //int start = Math.max(offsets[curTable]+1, state.tupleIndices[curTable]);
            //int end = Math.max(offsets[curTable]+1, tupleIndices[curTable]);
        	int start = Math.max(0, state.tupleIndices[curTable]);
        	int end = tupleIndices[curTable];
            int distance = end-start;
            reward += distance * curWeight;
        }
        */
        //double reward = (double)nrResultTuples/budget;
        // Only shareable progress
        /*
        int firstTable = order[0];
        reward = (tupleIndices[firstTable] - state.tupleIndices[firstTable])/
        		cardinalities[firstTable];
        */
        /*
        double resultTupleReward = (double)nrResultTuples/budget;
        int firstTable = order[0];
        int firstCard = cardinalities[firstTable];
        double sharedProgressReward = (double)(tupleIndices[firstTable] 
        		- state.tupleIndices[firstTable])/firstCard;
        */
        // Update state
        state.lastIndex = joinIndex;
        state.lastMove = move;
        for (int tableCtr=0; tableCtr<nrJoined; ++tableCtr) {
        	state.tupleIndices[tableCtr] = tupleIndices[tableCtr];
        }
        // Update progress with final state
        tracker.updateProgress(joinOrder, state);
        // Produce logging output
        log("Table offsets:\t" + Arrays.toString(offsets));
        log("Reward:\t" + reward);
        return reward;
    }
    @Override
    public boolean isFinished() {
    	return tracker.isFinished;
    }
    /**
     * Output log text unless the maximal number
     * of log entries has already been reached.
     * 
     * @param logEntry	text to output
     */
    void log(String logEntry) {
    	if (logCtr < LoggingConfig.MAX_JOIN_LOGS) {
    		++logCtr;
    		System.out.println(logCtr + "\t" + logEntry);
    	}
    }
}