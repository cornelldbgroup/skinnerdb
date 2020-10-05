package joining.parallel.plan;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import expressions.ExpressionInfo;
import expressions.compilation.KnaryBoolEval;
import joining.join.JoinDoubleWrapper;
import joining.join.JoinIndexWrapper;
import joining.join.JoinIntWrapper;
import joining.parallel.join.JoinDoublePartitionWrapper;
import joining.parallel.join.JoinIntPartitionWrapper;
import joining.parallel.join.JoinPartitionIndexWrapper;
import joining.plan.JoinOrder;
import net.sf.jsqlparser.expression.Expression;
import predicate.NonEquiNode;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import statistics.JoinStats;
import types.TypeUtil;

import java.util.*;

/**
 * Represents a right deep query plan, characterized
 * by a join order and by the time at which predicates
 * are evaluated.
 *
 * @author Anonymous
 *
 */
public class RightDeepPartitionPlan {
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
    public RightDeepPartitionPlan(QueryInfo query, Map<Expression, NonEquiNode> evalMap, JoinOrder joinOrder)
            throws Exception {
        // Count generated plan
        int nrTables = query.nrJoined;
        this.joinOrder = joinOrder;
        int[] order = joinOrder.order;
        splitStrategies = new int[order.length];
        // Initialize remaining predicates
        List<ExpressionInfo> remainingEquiPreds = new ArrayList<>(query.equiJoinPreds);
        List<ExpressionInfo> remainingPreds = new ArrayList<>();
        for (ExpressionInfo predInfo : query.wherePredicates) {
            if (predInfo.aliasesMentioned.size()>1) {
                remainingPreds.add(predInfo);
            }
        }
        // Initialize applicable predicates
        joinIndices = new ArrayList<>();
        applicablePreds = new ArrayList<>();
        nonEquiNodes = new ArrayList<>();

        Set<Integer> availableTables = new HashSet<>();
        for (int tableCtr = 0; tableCtr < nrTables; ++tableCtr) {
            joinIndices.add(new ArrayList<>());
            applicablePreds.add(new ArrayList<>());
            nonEquiNodes.add(new ArrayList<>());
            availableTables.add(tableCtr);
        }
        int noEquiPredsId = query.equiJoinPreds.size();
        // Iterate over join order positions, removing tables
        for (int joinCtr = 0; joinCtr < nrTables; ++joinCtr) {
            int nextTable = order[joinCtr];
            // Iterate over remaining equi-join predicates
            Iterator<ExpressionInfo> equiPredsIter = remainingEquiPreds.iterator();
            boolean first = true;
            while (equiPredsIter.hasNext()) {
                ExpressionInfo equiPred = equiPredsIter.next();
                if (availableTables.containsAll(
                        equiPred.aliasIdxMentioned) && equiPred.aliasIdxMentioned.contains(nextTable)) {
                    // initialize split strategy
                    if (first) {
                        splitStrategies[nextTable] = equiPred.pid;
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
                splitStrategies[nextTable] = noEquiPredsId;
                noEquiPredsId++;
            }
            // Iterate over remaining other predicates
            Iterator<ExpressionInfo> generalPredsIter =
                    remainingPreds.iterator();
            while (generalPredsIter.hasNext()) {
                ExpressionInfo pred = generalPredsIter.next();
                if (availableTables.containsAll(
                        pred.aliasIdxMentioned) && pred.aliasIdxMentioned.contains(nextTable)) {
                    NonEquiNode nonEquiNode = evalMap.get(
                            pred.finalExpression);
                    nonEquiNodes.get(joinCtr).add(nonEquiNode);
                    generalPredsIter.remove();
                }
            }
            availableTables.remove(nextTable);
        } // over join positions
    }
    @Override
    public String toString() {
        return "Join indices:\t" + joinIndices.toString() + System.lineSeparator() +
                "Other preds:\t" + applicablePreds.toString();
    }
}
