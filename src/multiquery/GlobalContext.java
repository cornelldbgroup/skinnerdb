package multiquery;

import catalog.CatalogManager;
import expressions.ExpressionInfo;
import joining.plan.JoinOrder;
import joining.result.ResultTuple;
import query.QueryInfo;
import utils.Pair;

import java.util.*;

public class GlobalContext {

    /**
     * common join info
     */
    public static HashMap<HashSet<Integer>, HashMap<Integer, List<HashSet<Integer>>>> commonJoins;

    public static HashMap<String, Integer> tableOrder;

    public static boolean[] queryStatus;

    public static int nrQuery = 0;

    public static int firstUnfinishedNum  = 0;

    public static String[][] aliasesTable;

    public static QueryInfo[] queryInfos;

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

    public static void initCommonJoin(QueryInfo[] queries) {
        commonJoins = new HashMap<>();
        tableOrder = new HashMap<>();
        nrQuery = queries.length;
        queryInfos = queries;
        aliasesTable = new String[nrQuery][];
        int tableGlobalIdx = 0;
        for (String tableName : CatalogManager.currentDB.nameToTable.keySet()) {
            tableOrder.put(tableName, tableGlobalIdx);
            tableGlobalIdx++;
        }
        //default status is finish
        queryStatus = new boolean[nrQuery];
        for(int i = 0; i < queries.length ; i++)  {
            QueryInfo query = queries[i];
            aliasesTable[i] = new String[query.nrJoined];
            for(int j = 0; j < query.nrJoined ; j++)
                aliasesTable[i][j] = query.aliasToTable.get(query.aliases[j]);
            Set<Integer> unaryTables = new HashSet<>();
            //tables which are involved in unary predicates
            for(ExpressionInfo expressionInfo: query.unaryPredicates) {
                unaryTables.addAll(expressionInfo.aliasIdxMentioned);
            }
            //join tables
            for(Set<Integer> joins: query.joinedIndices) {
                if(joins.size() == 0)
                    continue;
                int firstIdx = 0;
                int secondIdx = 0;
                boolean firstIt = true;
                for(Integer tableIdx: joins) {
                    if (firstIt)
                        firstIdx = tableIdx;
                    else
                        secondIdx = tableIdx;
                    firstIt = false;
                }
                if(firstIdx > secondIdx) {
                    int tmp = secondIdx;
                    secondIdx = firstIdx;
                    firstIdx = tmp;
                }
//                Pair<Integer, Integer> pair = new Pair<Integer, Integer>(firstIdx, secondIdx);
                HashSet<Integer> joinIdx = new HashSet<>();
                joinIdx.addAll(joins);
                if(!unaryTables.contains(firstIdx) && !unaryTables.contains(secondIdx)) {
                    String table1 = query.aliasToTable.get(query.aliases[firstIdx]);
                    String table2 = query.aliasToTable.get(query.aliases[secondIdx]);
                    //System.out.println(table1 + " " + table2);
                    int realIdx1 = tableOrder.get(table1);
                    int realIdx2 = tableOrder.get(table2);
                    HashSet<Integer> realIndices = new HashSet<>();
                    realIndices.add(realIdx1);
                    realIndices.add(realIdx2);
                    commonJoins.putIfAbsent(realIndices, new HashMap<>());
                    commonJoins.get(realIndices).putIfAbsent(query.queryNum, new ArrayList<>());
                    commonJoins.get(realIndices).get(query.queryNum).add(joinIdx);
                }
            }
        }
        System.out.println("reuse join candidates:");
        commonJoins.forEach((i, j) -> System.out.println("table:" + i.toString() + ", involved queries" + j.toString()));
        tableOrder.forEach((i, j) -> System.out.println(i +", " + j));
    }

    public static void aheadFirstUnfinish() {
        for(int i = 0; i < nrQuery; i++) {
            firstUnfinishedNum = (firstUnfinishedNum + 1) % nrQuery;
            //firstUnfinishedNum = i;
            if(!queryStatus[firstUnfinishedNum])
                return;
        }
        firstUnfinishedNum = -1;
    }

    public static int findGlobalIdxByTableName(String tableName) {
        return tableOrder.get(tableName);
    }

    public static boolean checkStatus() {
        for(boolean status:queryStatus) {
            if (!status)
                return false;
        }
        return true;
    }
}