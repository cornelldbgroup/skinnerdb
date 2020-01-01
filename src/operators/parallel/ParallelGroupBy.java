package operators.parallel;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import data.ColumnData;
import data.IntData;
import operators.Group;
import operators.OperatorUtils;
import operators.RowRange;
import postprocessing.IndexRange;
import query.ColumnRef;
import types.SQLtype;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelGroupBy {
    /**
     * Iterates over rows of input colunms (must have the
     * same cardinality) and calculates consecutive
     * group values that are stored in target column.
     *
     * @param sourceRefs	source column references
     * @param targetRef		store group ID in that column
     * @return				number of groups
     * @throws Exception
     */
    public static Map<Group, GroupIndex> execute(Collection<ColumnRef> sourceRefs,
                              ColumnRef targetRef) throws Exception {
        // Register result column
        String targetTbl = targetRef.aliasName;
        String targetCol = targetRef.columnName;
        ColumnInfo targetInfo = new ColumnInfo(targetCol,
                SQLtype.INT, false, false, false, false);
        CatalogManager.currentDB.nameToTable.get(targetTbl).addColumn(targetInfo);
        // Generate result column and load it into buffer
        String firstSourceTbl = sourceRefs.iterator().next().aliasName;
        int cardinality = CatalogManager.getCardinality(firstSourceTbl);
        IntData groupData = new IntData(cardinality);
        BufferManager.colToData.put(targetRef, groupData);
        // Get data of source columns
        List<ColumnData> sourceCols = new ArrayList<>();
        for (ColumnRef srcRef : sourceRefs) {
            sourceCols.add(BufferManager.getData(srcRef));
        }
        // Fill result column
        Map<Group, GroupIndex> groupIndexListMap = new HashMap<>();
//        List<RowRange> batches = OperatorUtils.split(cardinality);
//        batches.parallelStream().map(batch -> {
//            Map<Group, GroupIndex> batchMap = new HashMap<>();
//            for (int rowCtr = batch.firstTuple; rowCtr < batch.lastTuple; ++rowCtr) {
//                Group curGroup = new Group(rowCtr, sourceCols);
//                GroupIndex groupIndex = batchMap.computeIfAbsent(curGroup, group -> {
//                    int nextGroupID = batchMap.size();
//                    return new GroupIndex(nextGroupID);
//                });
//                groupIndex.addRow(rowCtr);
//            }
//            return batchMap;
//        }).sequential().forEach(batchMap -> batchMap.forEach((curGroup, curIndex) -> {
//            GroupIndex groupIndex = groupIndexListMap.computeIfAbsent(curGroup, group -> {
//                int nextGroupID = groupIndexListMap.size();
//                return new GroupIndex(nextGroupID);
//            });
//            groupIndex.merge(curIndex);
//        }));

//        ConcurrentMap<Group, List<Group>> resultSet = IntStream.range(0, cardinality).parallel().
//                mapToObj(row -> new Group(row, sourceCols))
//                .collect(Collectors.groupingByConcurrent(Function.identity()));



        for (int rowCtr = 0; rowCtr < cardinality; ++rowCtr) {
            Group curGroup = new Group(rowCtr, sourceCols);
            GroupIndex groupIndex = groupIndexListMap.computeIfAbsent(curGroup, group -> {
                int nextGroupID = groupIndexListMap.size();
                return new GroupIndex(nextGroupID);
            });
            groupIndex.addRow(rowCtr);
            groupData.data[rowCtr] = groupIndex.gid;
        }
        // Update catalog statistics
        CatalogManager.updateStats(targetTbl);
        // Retrieve data for
        return groupIndexListMap;
    }
}
