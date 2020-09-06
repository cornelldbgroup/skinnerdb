package operators.parallel;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import config.GeneralConfig;
import config.ParallelConfig;
import data.ColumnData;
import data.IntData;
import expressions.ExpressionInfo;
import indexing.Index;
import indexing.Indexer;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntIndexRange;
import joining.parallel.indexing.IntPartitionIndex;
import joining.parallel.indexing.PartitionIndex;
import operators.Group;
import operators.OperatorUtils;
import operators.RowRange;
import postprocessing.IndexRange;
import query.ColumnRef;
import query.QueryInfo;
import types.SQLtype;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    public static int execute(Collection<ColumnRef> sourceRefs,
                              ColumnRef targetRef, QueryInfo query) throws Exception {
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

        int nrKeys = 1;
        for (ExpressionInfo expression : query.groupByExpressions) {
            Index index = BufferManager.colToIndex.get(expression.columnsMentioned.iterator().next());
            if (index instanceof IntPartitionIndex) {
                nrKeys *= ((IntPartitionIndex)index).nrKeys;
            }
            else if (index instanceof DoublePartitionIndex) {
                nrKeys *= ((DoublePartitionIndex)index).nrKeys;
            }
            else {
                nrKeys = cardinality;
            }
        }

        boolean isDense = nrKeys < 100;
        int nrGroups;
        List<GroupIndexRange> batches = split(cardinality);
        if (isDense) {
            Map<Group, Integer> groupToID = new HashMap<>(cardinality);
            batches.parallelStream().forEach(batch -> {
                int first = batch.firstTuple;
                // Evaluate predicate for each table row
                for (int rowCtr = first; rowCtr <= batch.lastTuple; ++rowCtr) {
                    Group group = new Group(rowCtr, sourceCols);
                    batch.add(group, rowCtr - first);
                }
            });
            for (GroupIndexRange batch : batches) {
                for (Map.Entry<Group, Integer> entry : batch.valuesMap.entrySet()) {
                    int nextId = groupToID.size();
                    groupToID.putIfAbsent(entry.getKey(), nextId);
                }
            }
            batches.parallelStream().forEach(batch -> {
                int first = batch.firstTuple;
                // Evaluate predicate for each table row
                Group[] groups = batch.groups;
                for (int rowCtr = first; rowCtr <= batch.lastTuple; ++rowCtr) {
                    int groupID = groupToID.get(groups[rowCtr - first]);
                    groupData.data[rowCtr] = groupID;
                }
            });
            nrGroups = groupToID.size();
        }
        else {
            ConcurrentMap <Group, Integer> curGroupToID = new ConcurrentHashMap<>(cardinality);
            AtomicInteger nextID = new AtomicInteger(0);
            batches.parallelStream().forEach(batch -> {
                int first = batch.firstTuple;
                // Evaluate predicate for each table row
                for (int rowCtr = first; rowCtr <= batch.lastTuple; ++rowCtr) {
                    Group group = new Group(rowCtr, sourceCols);
                    Integer groupID = curGroupToID.putIfAbsent(group, 0);
                    if (groupID == null) {
                        groupID = nextID.getAndIncrement();
                        curGroupToID.put(group, groupID);
                    }
                    groupData.data[rowCtr] = groupID;
                }
            });
            nrGroups = curGroupToID.size();
        }

        // Update catalog statistics
        CatalogManager.updateStats(targetTbl);
        // Retrieve data for
        return nrGroups;
    }

    public static int executeByIndex(Collection<ColumnRef> sourceRefs,
                              ColumnRef targetRef, QueryInfo query) throws Exception {
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
        sourceCols.remove(0);

        ColumnRef queryRef = query.groupByExpressions.iterator().next().columnsMentioned.iterator().next();
        ColumnRef firstRef = sourceRefs.iterator().next();

        ColumnInfo columnInfo = query.colRefToInfo.get(queryRef);
        String tableName = query.aliasToTable.get(queryRef.aliasName);
        String columnName = queryRef.columnName;
        ColumnRef columnRef = new ColumnRef(tableName, columnName);
        Index index = BufferManager.colToIndex.getOrDefault(columnRef, null);
        PartitionIndex partitionIndex = null;

        Index groupIndex = Indexer.partitionIndex(firstRef, queryRef, partitionIndex,
                columnInfo.isPrimary, !GeneralConfig.isParallel, false);
        int nrGroups = -1;
        if (groupIndex instanceof IntPartitionIndex) {
            IntPartitionIndex intIndex = (IntPartitionIndex) groupIndex;
            int[] positions = intIndex.positions;
            int[] posArray = intIndex.keyToPositions.values().toIntArray();
            int[] nrKeys = new int[posArray.length + 1];
            IntStream.range(0, posArray.length).parallel().forEach(posIndex -> {
                int pos = posArray[posIndex];
                int nrVals = positions[pos];
                if (nrVals > 0) {
                    List<GroupIndexRange> batches = split(nrVals);
                    ConcurrentMap <Group, Integer> curGroupToID = new ConcurrentHashMap<>(nrVals);
                    AtomicInteger nextID = new AtomicInteger(0);

                    batches.parallelStream().forEach(batch -> {
                        int first = batch.firstTuple;
                        int last = batch.lastTuple;
                        for (int posCtr = first; posCtr <= last; ++posCtr) {
                            int row = positions[posCtr + pos + 1];
                            Group group = new Group(row, sourceCols);
                            Integer groupID = curGroupToID.putIfAbsent(group, 0);
                            if (groupID == null) {
                                groupID = nextID.getAndIncrement();
                                curGroupToID.put(group, groupID);
                            }
                            groupData.data[row] = groupID;
                        }
                    });
                    nrKeys[posIndex + 1] = curGroupToID.size();
                }
            });
            Arrays.parallelPrefix(nrKeys, Integer::sum);
            IntStream.range(0, posArray.length).parallel().forEach(posIndex -> {
                int pos = posArray[posIndex];
                int nrVals = positions[pos];
                int prefix = nrKeys[posIndex];
                if (nrVals > 0 && prefix > 0) {
                    List<GroupIndexRange> batches = split(nrVals);
                    batches.parallelStream().forEach(batch -> {
                        int first = batch.firstTuple;
                        int last = batch.lastTuple;
                        for (int posCtr = first; posCtr <= last; ++posCtr) {
                            int row = positions[posCtr + pos + 1];
                            groupData.data[row] += prefix;
                        }
                    });
                }
            });
            nrGroups = nrKeys[nrKeys.length - 1];
        }
        else if (groupIndex instanceof DoublePartitionIndex) {
            DoublePartitionIndex doubleIndex = (DoublePartitionIndex) groupIndex;
            int[] positions = doubleIndex.positions;
            int[] posArray = doubleIndex.keyToPositions.values().toIntArray();
            int[] nrKeys = new int[posArray.length + 1];
            IntStream.range(0, posArray.length).parallel().forEach(posIndex -> {
                int pos = posArray[posIndex];
                int nrVals = positions[pos];
                if (nrVals > 0) {
                    List<GroupIndexRange> batches = split(nrVals);
                    ConcurrentMap <Group, Integer> curGroupToID = new ConcurrentHashMap<>(nrVals);
                    AtomicInteger nextID = new AtomicInteger(0);
                    batches.parallelStream().forEach(batch -> {
                        int first = batch.firstTuple;
                        int last = batch.lastTuple;
                        for (int rowCtr = first; rowCtr <= last; ++rowCtr) {
                            int row = positions[rowCtr + pos + 1];
                            Group group = new Group(row, sourceCols);
                            Integer groupID = curGroupToID.putIfAbsent(group, 0);
                            if (groupID == null) {
                                groupID = nextID.getAndIncrement();
                                curGroupToID.put(group, groupID);
                            }
                            groupData.data[row] = groupID;
                        }
                    });
                    nrKeys[posIndex + 1] = curGroupToID.size();
                }
            });
            Arrays.parallelPrefix(nrKeys, Integer::sum);
            IntStream.range(0, posArray.length).parallel().forEach(posIndex -> {
                int pos = posArray[posIndex];
                int nrVals = positions[pos];
                int prefix = nrKeys[posIndex];
                if (nrVals > 0 && prefix > 0) {
                    List<GroupIndexRange> batches = split(nrVals);
                    batches.parallelStream().forEach(batch -> {
                        int first = batch.firstTuple;
                        int last = batch.lastTuple;
                        for (int posCtr = first; posCtr <= last; ++posCtr) {
                            int row = positions[posCtr + pos + 1];
                            groupData.data[row] += prefix;
                        }
                    });
                }
            });
            nrGroups = nrKeys[nrKeys.length - 1];
        }



        // Update catalog statistics
        CatalogManager.updateStats(targetTbl);
        // Retrieve data for
        return nrGroups;
    }

    public static Map<Group, GroupIndex> executeIndex(Collection<ColumnRef> sourceRefs,
                                                 ColumnRef targetRef, Index index) throws Exception {
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
        int[] gids = index.groupIds;
        int[] positions = index.positions;
        IntStream.range(0, gids.length).parallel().forEach(gid -> {
            int pos = gids[gid];
            int groupCard = positions[pos];
            for (int i = pos + 1; i <= pos + groupCard; i++) {
                int rowCtr = positions[i];
                groupData.data[rowCtr] = gid;
            }
        });
        // Update catalog statistics
        CatalogManager.updateStats(targetTbl);
        // Retrieve data for
        return null;
    }

    /**
     * Splits table with given cardinality into tuple batches
     * according to the configuration for joining.parallel processing.
     *
     * @return list of row ranges (batches)
     */
    static List<GroupIndexRange> split(int cardinality) {
        List<GroupIndexRange> batches = new ArrayList<>();
        int batchSize = Math.max(ParallelConfig.PARALLEL_SIZE, cardinality / 300);
        for (int batchCtr = 0; batchCtr * batchSize < cardinality;
             ++batchCtr) {
            int startIdx = batchCtr * batchSize;
            int tentativeEndIdx = startIdx + batchSize - 1;
            int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
            GroupIndexRange groupIndexRange = new GroupIndexRange(startIdx, endIdx, batchCtr);
            batches.add(groupIndexRange);
        }
        return batches;
    }
}
