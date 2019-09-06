package multiquery;

import expressions.ExpressionInfo;
import query.QueryInfo;
import utils.Pair;

import java.util.*;

public class GlobalContext {

    /**
     * common join info
     */
    public static HashMap<Pair<Integer, Integer>, Set<Integer>> commonJoins;

    public static boolean[] queryStatus;

    public static int nrQuery = 0;

    public static int firstUnfinishedNum  = 0;

//    For now, we don't consider share binary predicates
//    public static void initCommonUnary(List<QueryInfo> queries) {
//        for (QueryInfo query : queries) {
//            List<List<ExpressionInfo>> unaryPredicates = new ArrayList<>(query.nrJoined);
//            for(ExpressionInfo expressionInfo: query.unaryPredicates) {
//                for(int idx : expressionInfo.aliasIdxMentioned) {
//                    List<ExpressionInfo> expressionInfos = unaryPredicates.get(idx);
//                    if(expressionInfos == null) {
//                        expressionInfos = new ArrayList<>();
//                        unaryPredicates.add(expressionInfos);
//                    }
//                    expressionInfos.add(expressionInfo);
//                }
//            }
//
//            for(Set<Integer> joins: query.joinedIndices) {
//                boolean first = true;
//                PredicateConnection connection = new PredicateConnection();
//                Map<String, String> tableMap = query.aliasToTable;
//                for(Integer tableIdx: joins) {
//                    if(first) {
//                        connection.setLeftTableIdx(tableIdx);
//                        connection.setLeftTableName(tableMap.get(query.aliases[tableIdx]));
//                    } else {
//                        connection.setRightTableIdx(tableIdx);
//                        connection.setRightTableName(tableMap.get(query.aliases[tableIdx]));
//                    }
//                    first = false;
//
//                }
//
//            }
//
//
//            //connection.setUnaryExpression(unaryPredicates.get(tableIdx));
//
//            //query.wherePredicates;
//
//            PlainSelect select = query.plainSelect;
//            select.getJoins();
//
//            //select.getJoins().forEach(j -> System.out.println( j.getRightItem()));
//        }
//    }

    public static void initCommonJoin(List<QueryInfo> queries) {
        commonJoins = new HashMap<>();
        nrQuery = queries.size();
        //default status is finish
        queryStatus = new boolean[nrQuery];
        for (QueryInfo query : queries) {
            List<ExpressionInfo> unaryExpressions = query.unaryPredicates;
            Set<Integer> unaryTables = new HashSet<>();
            //tables which are involved in unary predicates
            for(ExpressionInfo unaryExpression : unaryExpressions) {
                for(ExpressionInfo expressionInfo: query.unaryPredicates) {
                    unaryTables.addAll(expressionInfo.aliasIdxMentioned);
                }
            }

            for(Set<Integer> joins: query.joinedIndices) {
                Pair<Integer, Integer> pair = new Pair<Integer, Integer>();
                int i = 0;
                for(Integer tableIdx: joins) {
                    if (i == 0)
                        pair.setFirst(tableIdx);
                    else
                        pair.setSecond(tableIdx);
                    i++;
                }
                if(!unaryTables.contains(pair.getFirst()) && !unaryTables.contains(pair.getSecond())) {
                    commonJoins.putIfAbsent(pair, new HashSet<Integer>());
                    commonJoins.get(pair).add(query.queryNum);
                }
            }
        }
        System.out.println("reuse join candidates:");
        commonJoins.forEach((i, j) -> System.out.println("table:" + i.getFirst() + ", join table" + i.getSecond() + ", involved queries" + j.toString()));
    }

    public static void aheadFirstUnfinish() {
        for(int i = 0; i < nrQuery; i++) {
            firstUnfinishedNum = (firstUnfinishedNum + 1) % nrQuery;
            if(!queryStatus[firstUnfinishedNum])
                break;
        }
        firstUnfinishedNum = -1;
    }
}
