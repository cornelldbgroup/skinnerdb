package joining.parallel.plan;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import expressions.ExpressionInfo;
import expressions.compilation.KnaryBoolEval;
import joining.join.JoinDoubleWrapper;
import joining.join.JoinIntWrapper;
import joining.parallel.join.*;
import joining.plan.JoinOrder;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
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
 * @author Anonymous
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
	 * None-equi join predicates represented by a tree.
	 */
	public final List<List<NonEquiNode>> nonEquiNodes;
	/**
	 * Split strategy for each split table.
	 */
	public final int[] splitStrategies;

	/**
	 * Determines the join order positions at which to
	 * evaluate specific equality and other predicates.
	 *
	 * @param query			query to process
	 * @param evalMap		maps Boolean expressions to evaluators
	 * @param joinOrder		join order
	 * @throws Exception
	 */
	public LeftDeepPartitionPlan(QueryInfo query, Map<Expression, NonEquiNode> evalMap,
								 JoinOrder joinOrder)
					throws Exception {
		// Count generated plan
		int nrTables = query.nrJoined;
		this.joinOrder = joinOrder;
		int[] order = joinOrder.order;
		splitStrategies = new int[order.length];
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
		nonEquiNodes = new ArrayList<>();
		for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
			joinIndices.add(new ArrayList<>());
			applicablePreds.add(new ArrayList<>());
			nonEquiNodes.add(new ArrayList<>());
		}
		// Iterate over join order positions, adding tables
		Set<Integer> availableTables = new HashSet<>();
		int nrEquiJoins = query.equiJoinPreds.size();
		for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
			int nextTable = order[joinCtr];
			availableTables.add(nextTable);
			// Iterate over remaining equi-join predicates
			Iterator<ExpressionInfo> equiPredsIter = remainingEquiPreds.iterator();
			boolean first = true;
			while (equiPredsIter.hasNext()) {
				ExpressionInfo equiPred = equiPredsIter.next();
				if (availableTables.containsAll(
						equiPred.aliasIdxMentioned)) {
					// initialize split strategy
					if (first) {
						Iterator<Integer> tableIter = equiPred.aliasIdxMentioned.iterator();
						int table1 = tableIter.next();
						int table2 = tableIter.next();
						int smallTable = Math.min(table1, table2);
						int times = smallTable == nextTable ? 0 : 1;
						int firstTable = times * nrEquiJoins + equiPred.pid;
						splitStrategies[nextTable] = firstTable;
						first = false;
					}
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
			if (first) {
				splitStrategies[nextTable] = nrEquiJoins * 2 + nextTable;
//				noEquiPredsId++;
			}
			// Iterate over remaining other predicates
			Iterator<ExpressionInfo> generalPredsIter =
					remainingPreds.iterator();
			while (generalPredsIter.hasNext()) {
				ExpressionInfo pred = generalPredsIter.next();
				if (availableTables.containsAll(
						pred.aliasIdxMentioned)) {
					NonEquiNode nonEquiNode = evalMap.get(
							pred.finalExpression);
					nonEquiNodes.get(joinCtr).add(nonEquiNode);
					generalPredsIter.remove();
				}
			}

		} // over join positions
	}

//	public LeftDeepPartitionPlan(
//			QueryInfo query,
//			Map<Expression, NonEquiNode> evalMap,
//			JoinOrder joinOrder,
//			int tid)
//			throws Exception {
//		// Count generated plan
//		int nrTables = query.nrJoined;
//		this.joinOrder = joinOrder;
//		int[] order = joinOrder.order;
//		splitStrategies = new int[order.length];
//		// Initialize remaining predicates
//		List<ExpressionInfo> remainingEquiPreds = new ArrayList<>(query.equiJoinPreds);
//		List<ExpressionInfo> remainingPreds = new ArrayList<>();
//		// Initialize nonEquiJoinPredicates
//		for (ExpressionInfo predInfo : query.nonEquiJoinPreds) {
//			if (predInfo.aliasesMentioned.size()>1) {
//				remainingPreds.add(predInfo);
//			}
//		}
//		// Initialize applicable predicates
//		joinIndices = new ArrayList<>();
//		applicablePreds = new ArrayList<>();
//		nonEquiNodes = new ArrayList<>();
//		for (int tableCtr=0; tableCtr<nrTables; ++tableCtr) {
//			joinIndices.add(new ArrayList<>());
//			applicablePreds.add(new ArrayList<>());
//			nonEquiNodes.add(new ArrayList<>());
//		}
//		// Iterate over join order positions, adding tables
//		Set<Integer> availableTables = new HashSet<>();
//		int noEquiPredsId = query.equiJoinPreds.size();
//		for (int joinCtr=0; joinCtr<nrTables; ++joinCtr) {
//			int nextTable = order[joinCtr];
//			availableTables.add(nextTable);
//			// Iterate over remaining equi-join predicates
//			Iterator<ExpressionInfo> equiPredsIter = remainingEquiPreds.iterator();
//			boolean first = true;
//			while (equiPredsIter.hasNext()) {
//				ExpressionInfo equiPred = equiPredsIter.next();
//				if (availableTables.containsAll(
//						equiPred.aliasIdxMentioned)) {
//					// initialize split strategy
//					if (first) {
//						splitStrategies[nextTable] = equiPred.pid;
//						first = false;
//					}
//					switch (equiPred.type) {
//						case INT:
//							switch (tid) {
//								case 0:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper0(equiPred, order));
//									break;
//								case 1:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper1(equiPred, order));
//									break;
//								case 2:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper2(equiPred, order));
//									break;
//								case 3:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper3(equiPred, order));
//									break;
//								case 4:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper4(equiPred, order));
//									break;
//								case 5:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper5(equiPred, order));
//									break;
//								case 6:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper6(equiPred, order));
//									break;
//								case 7:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper7(equiPred, order));
//									break;
//								case 8:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper8(equiPred, order));
//									break;
//								case 9:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper9(equiPred, order));
//									break;
//								case 10:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper10(equiPred, order));
//									break;
//								case 11:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper11(equiPred, order));
//									break;
//								case 12:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper12(equiPred, order));
//									break;
//								case 13:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper13(equiPred, order));
//									break;
//								case 14:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper14(equiPred, order));
//									break;
//								case 15:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper15(equiPred, order));
//									break;
//								case 16:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper16(equiPred, order));
//									break;
//								case 17:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper17(equiPred, order));
//									break;
//								case 18:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper18(equiPred, order));
//									break;
//								case 19:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper19(equiPred, order));
//									break;
//								case 20:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper20(equiPred, order));
//									break;
//								case 21:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper21(equiPred, order));
//									break;
//								case 22:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper22(equiPred, order));
//									break;
//								case 23:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper23(equiPred, order));
//									break;
//								case 24:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper24(equiPred, order));
//									break;
//								case 25:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper25(equiPred, order));
//									break;
//								case 26:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper26(equiPred, order));
//									break;
//								case 27:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper27(equiPred, order));
//									break;
//								case 28:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper28(equiPred, order));
//									break;
//								default:
//									joinIndices.get(joinCtr).add(new JoinIntPartitionWrapper29(equiPred, order));
//									break;
//							}
//							break;
//						case DOUBLE:
//							joinIndices.get(joinCtr).add(new JoinDoublePartitionWrapper(equiPred, order));
//							break;
//						default:
//							throw new SQLexception("Error - no support for equality "
//									+ "join predicates between columns of type " + equiPred.type);
//					}
//					equiPredsIter.remove();
//				}
//			}
//			if (first) {
//				splitStrategies[nextTable] = noEquiPredsId;
//				noEquiPredsId++;
//			}
//			// Iterate over remaining other predicates
//			Iterator<ExpressionInfo> generalPredsIter =
//					remainingPreds.iterator();
//			while (generalPredsIter.hasNext()) {
//				ExpressionInfo pred = generalPredsIter.next();
//				if (availableTables.containsAll(
//						pred.aliasIdxMentioned)) {
//					NonEquiNode nonEquiNode = evalMap.get(
//							pred.finalExpression);
//					nonEquiNodes.get(joinCtr).add(nonEquiNode);
//					generalPredsIter.remove();
//				}
//			}
//
//		} // over join positions
//	}

	@Override
	public String toString() {
		return "Join indices:\t" + joinIndices.toString() + System.lineSeparator() +
				"Other preds:\t" + applicablePreds.toString();
	}
}