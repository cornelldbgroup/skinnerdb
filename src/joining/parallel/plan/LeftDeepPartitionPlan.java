package joining.parallel.plan;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import expressions.ExpressionInfo;
import expressions.compilation.KnaryBoolEval;
import joining.join.JoinDoubleWrapper;
import joining.join.JoinIntWrapper;
import joining.parallel.join.JoinDoublePartitionWrapper;
import joining.parallel.join.JoinIntPartitionWrapper;
import joining.plan.JoinOrder;
import joining.parallel.join.DPJoin;
import joining.parallel.join.JoinPartitionIndexWrapper;
import net.sf.jsqlparser.expression.Expression;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import types.TypeUtil;

import java.util.*;

/**
 * Represents a left deep query plan, characterized
 * by a join order and by the time at which predicates
 * are evaluated.
 * 
 * @author Ziyun Wei
 *
 */
public class LeftDeepPartitionPlan {
	/**
	 * Order of tables in which they are joined.
	 */
	public final JoinOrder joinOrder;
	/**
	 * Associates join order positions with applicable
	 * indices (used to skip tuples in the corresponding
	 * table which do not satisfy equality join predicates).
	 */
	public final List<List<JoinPartitionIndexWrapper>> joinIndices;

	/**
	 * Associates join order positions with applicable
	 * predicates (null if no new predicate is applicable).
	 */
	public final List<List<KnaryBoolEval>> applicablePreds;

	/**
	 * Determines the join order positions at which to
	 * evaluate specific equality and other predicates.
	 *
	 * @param query			query to process
	 * @param evalMap		maps Boolean expressions to evaluators
	 * @param joinOrder		join order
	 * @throws Exception
	 */
	public LeftDeepPartitionPlan(QueryInfo query, Map<Expression, KnaryBoolEval> evalMap, JoinOrder joinOrder)
					throws Exception {
		// Count generated plan
		int nrTables = query.nrJoined;
		this.joinOrder = joinOrder;
		int[] order = joinOrder.order;
		// Initialize remaining predicates
		List<ExpressionInfo> remainingEquiPreds = new ArrayList<>(query.equiJoinPreds);
		List<ExpressionInfo> remainingPreds = new ArrayList<>();
		// Initialize nonEquiJoinPredicates
		for (ExpressionInfo predInfo : query.nonEquiJoinPreds) {
			if (predInfo.aliasesMentioned.size()>1) {
				remainingPreds.add(predInfo);
			}
		}
		// Initialize applicable predicates
		joinIndices = new ArrayList<>();
		applicablePreds = new ArrayList<>();
		for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
			joinIndices.add(new ArrayList<>());
			applicablePreds.add(new ArrayList<>());
		}
		// Iterate over join order positions, adding tables
		Set<Integer> availableTables = new HashSet<>();
		for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
			int nextTable = order[joinCtr];
			availableTables.add(nextTable);
			// Iterate over remaining equi-join predicates
			Iterator<ExpressionInfo> equiPredsIter = remainingEquiPreds.iterator();
			while (equiPredsIter.hasNext()) {
				ExpressionInfo equiPred = equiPredsIter.next();
				if (availableTables.containsAll(
						equiPred.aliasIdxMentioned)) {
					switch (equiPred.type) {
						case INT:
							joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper(equiPred, order));
							break;
						case DOUBLE:
							joinIndices.get(joinCtr).add(new JoinDoublePartitionWrapper(equiPred, order));
							break;
						default:
							throw new SQLexception("Error - no support for equality "
									+ "join predicates between columns of type " + equiPred.type);
					}
					equiPredsIter.remove();
				}
			}
			// Iterate over remaining other predicates
			Iterator<ExpressionInfo> generalPredsIter =
					remainingPreds.iterator();
			while (generalPredsIter.hasNext()) {
				ExpressionInfo pred = generalPredsIter.next();
				if (availableTables.containsAll(
						pred.aliasIdxMentioned)) {
					KnaryBoolEval evaluator = evalMap.get(
							pred.finalExpression);
					applicablePreds.get(joinCtr).add(evaluator);
					generalPredsIter.remove();
				}
			}

		} // over join positions
	}
	@Override
	public String toString() {
		return "Join indices:\t" + joinIndices.toString() + System.lineSeparator() +
				"Other preds:\t" + applicablePreds.toString();
	}
}