package joining.plan;

import java.util.*;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import expressions.ExpressionInfo;
import expressions.compilation.KnaryBoolEval;
import joining.join.JoinDoubleWrapper;
import joining.join.JoinIndexWrapper;
import joining.join.JoinIntWrapper;
import net.sf.jsqlparser.expression.Expression;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import statistics.JoinStats;
import types.TypeUtil;

/**
 * Represents a left deep query plan, characterized
 * by a join order and by the time at which predicates
 * are evaluated.
 * 
 * @author immanueltrummer
 *
 */
public class LeftDeepPlan {
	/**
	 * Order of tables in which they are joined.
	 */
	public final JoinOrder joinOrder;
	/**
	 * Associates join order positions with applicable
	 * indices (used to skip tuples in the corresponding
	 * table which do not satisfy equality join predicates).
	 */
	public final List<List<JoinIndexWrapper>> joinIndices;
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
	 * @param preSummary	summarizes pre-processing
	 * @param evalMap		maps Boolean expressions to evaluators
	 * @param order			join order
	 * @throws Exception
	 */
	public LeftDeepPlan(QueryInfo query, Context preSummary, 
			Map<Expression, KnaryBoolEval> evalMap, int[] order) 
					throws Exception {
		// Count generated plan
		++JoinStats.nrPlansTried;
		int nrTables = query.nrJoined;
		this.joinOrder = new JoinOrder(order);
		// Initialize remaining predicates
		List<ExpressionInfo> remainingEquiPreds = new ArrayList<>();
		remainingEquiPreds.addAll(query.equiJoinPreds);
		List<ExpressionInfo> remainingPreds = new ArrayList<>();
		for (ExpressionInfo predInfo : query.wherePredicates) {
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
		Set<Integer> availableTables = new HashSet<Integer>();
		for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
			int nextTable = order[joinCtr];
			availableTables.add(nextTable);
			// Iterate over remaining equi-join predicates
			Iterator<ExpressionInfo> equiPredsIter = remainingEquiPreds.iterator();
			while (equiPredsIter.hasNext()) {
				ExpressionInfo equiPred = equiPredsIter.next();
				if (availableTables.containsAll(
						equiPred.aliasIdxMentioned)) {
					Set<ColumnRef> joinCols = equiPred.columnsMentioned;
					// Distinguish column type and create corresponding wrapper
					ColumnRef firstQueryRef = joinCols.iterator().next();
					ColumnRef firstDBref = preSummary.columnMapping.get(firstQueryRef);
					ColumnInfo firstInfo = CatalogManager.getColumn(firstDBref);
					switch (TypeUtil.toJavaType(firstInfo.type)) {
					case INT:
						joinIndices.get(joinCtr).add(new JoinIntWrapper(
								query, preSummary, joinCols, order));
						break;
					case DOUBLE:
						joinIndices.get(joinCtr).add(new JoinDoubleWrapper(
								query, preSummary, joinCols, order));
						break;
					default:
						throw new SQLexception("Error - no support for equality "
								+ "join predicates between columns of type " +
								firstInfo.type);
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