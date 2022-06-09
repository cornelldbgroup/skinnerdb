package preprocessing;
import java.util.*;
import java.util.stream.Collectors;


import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.*;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import expressions.ExpressionInfo;
import indexing.Index;
import indexing.Indexer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import jni.JNIFilter;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.Filter;
import operators.IndexFilter;
import operators.IndexTest;
import operators.Materialize;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PreStats;

/**
 * Filters query tables via unary predicates and stores
 * result in newly created tables. Creates hash tables
 * for columns with binary equality join predicates.
 *
 * @author Anonymous
 *
 */
public class NewPreprocessor {
    static {
        try {
            System.load(GeneralConfig.JNI_PATH);
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
    /**
     * Whether an error occurred during last invocation.
     * This flag is used in cases where an error occurs
     * without an exception being thrown.
     */
    public static boolean hadError = false;
    /**
     * Whether to calculate performance.
     */
    public static boolean performance = false;
    /**
     * Whether to calculate performance.
     */
    public static boolean terminated = false;
    /**
     * Translates a column reference using a table
     * alias into one using the original table.
     *
     * @param query		meta-data about query
     * @param queryRef	reference to alias column
     * @return 			resolved column reference
     */
    static ColumnRef DBref(QueryInfo query, ColumnRef queryRef) {
        String alias = queryRef.aliasName;
        String table = query.aliasToTable.get(alias);
        String colName = queryRef.columnName;
        return new ColumnRef(table, colName);
    }
    /**
     * Executes pre-processing.
     *
     * @param query			the query to pre-process
     * @return 				summary of pre-processing steps
     */
    public static Context process(QueryInfo query) throws Exception {
        long dictionaryStart = System.currentTimeMillis();
        JNIFilter.fill(BufferManager.dictionary.strings);
        long dictionaryEnd = System.currentTimeMillis();
        System.out.println("Dictionary loaded: " + (dictionaryEnd - dictionaryStart));
        // Start counter
        long startMillis = System.currentTimeMillis();
        // Reset error flag
        hadError = false;
        terminated = false;
        // Collect columns required for joins and post-processing
        Set<ColumnRef> requiredCols = new HashSet<>();
        requiredCols.addAll(query.colsForJoins);
        requiredCols.addAll(query.colsForPostProcessing);
        log("Required columns: " + requiredCols);
        // Initialize pre-processing summary
        Context preSummary = new Context();
        // Initialize mapping for join and post-processing columns
        for (ColumnRef queryRef : requiredCols) {
            preSummary.columnMapping.put(queryRef,
                    DBref(query, queryRef));
        }
        // Initialize column mapping for unary predicate columns
        for (ExpressionInfo unaryPred : query.unaryPredicates) {
            for (ColumnRef queryRef : unaryPred.columnsMentioned) {
                preSummary.columnMapping.put(queryRef,
                        DBref(query, queryRef));
            }
        }
        // Initialize mapping from query alias to DB tables
        preSummary.aliasToFiltered.putAll(query.aliasToTable);
        boolean inCached = GeneralConfig.ISTESTCASE && PreConfig.IN_CACHE;
        log("Column mapping:\t" + preSummary.columnMapping.toString());
        // Iterate over query aliases
        query.aliasToTable.keySet().forEach(alias -> {
            long s1 = System.currentTimeMillis();
            // Collect required columns (for joins and post-processing) for this table
            List<ColumnRef> curRequiredCols = new ArrayList<>();
            for (ColumnRef requiredCol : requiredCols) {
                if (requiredCol.aliasName.equals(alias)) {
                    curRequiredCols.add(requiredCol);
                }
            }
            // Get applicable unary predicates
            ExpressionInfo curUnaryPred = null;
            for (ExpressionInfo exprInfo : query.unaryPredicates) {
                if (exprInfo.aliasesMentioned.contains(alias)) {
                    curUnaryPred = exprInfo;
                }
            }
            int size = 0;
            // Filter and project if enabled
            if (curUnaryPred != null && PreConfig.FILTER) {
                try {
                    //check if the predicate is in the cache
                    List<Integer> inCacheRows = null;
                    if (inCached) {
                        inCacheRows = applyCache(curUnaryPred);
                    }
                    if (inCacheRows == null) {
                        // Apply index to prune rows if possible
                        IndexFilter filter = applyIndex(
                                query, curUnaryPred, preSummary);
                        ExpressionInfo remainingPred = filter.remainingInfo;
                        String indexedFilterName = preSummary.aliasToFiltered.get(alias);;
                        size = CatalogManager.getCardinality(indexedFilterName);
                        if (size == 0) {
                            terminated = true;
                        }
                        // TODO: reinsert index usage
                        //ExpressionInfo remainingPred = curUnaryPred;
                        // Filter remaining rows by remaining predicate
                        if (remainingPred != null) {
                            filterProject(query, alias, filter,
                                    curRequiredCols, preSummary);
//							if (inCached && rows.size() > 0 && rows.get(0) >= 0 && filter.qualifyingRows.size() == 0) {
//								BufferManager.indexCache.putIfAbsent(curUnaryPred.pid, rows);
//							}
                            String filteredName = NamingConfig.FILTERED_PRE + alias;
                            size = CatalogManager.getCardinality(filteredName);
                            if (size == 0) {
                                terminated = true;
//                                break;
                            }
                        }
                        else {
                            String filteredName = NamingConfig.IDX_FILTERED_PRE + alias;
                            size = CatalogManager.getCardinality(filteredName);
                            if (size == 0) {
                                terminated = true;
//								break;
                            }
                        }
                    }
                    else {
                        // Materialize relevant rows and columns
                        String tableName = preSummary.aliasToFiltered.get(alias);
                        String filteredName = NamingConfig.FILTERED_PRE + alias;
                        List<String> columnNames = new ArrayList<>();
                        for (ColumnRef colRef : curRequiredCols) {
                            columnNames.add(colRef.columnName);
                        }
                        long timer1 = System.currentTimeMillis();
                        Materialize.execute(tableName, columnNames,
                                inCacheRows, null, filteredName, true);
                        long timer2 = System.currentTimeMillis();
                        System.out.println("Materializing after cache: " + filteredName + " took " + (timer2 - timer1) + " ms");
                        // Update pre-processing summary
                        for (ColumnRef srcRef : curRequiredCols) {
                            String columnName = srcRef.columnName;
                            ColumnRef resRef = new ColumnRef(filteredName, columnName);
                            preSummary.columnMapping.put(srcRef, resRef);
                        }
                        preSummary.aliasToFiltered.put(alias, filteredName);
                        int cardinality = CatalogManager.getCardinality(filteredName);
                        if (cardinality == 0) {
                            terminated = true;
//							break;
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error filtering " + alias);
                    e.printStackTrace();
                    hadError = true;
                }
            } else {
                String table = query.aliasToTable.get(alias);
                preSummary.aliasToFiltered.put(alias, table);
            }
            long s2 = System.currentTimeMillis();
            if (curUnaryPred != null) {
                System.out.println("Predicate: " + curUnaryPred + "\tSize: " + size + "\tTime: " + (s2 - s1));
            }
        });

        // Abort pre-processing if filtering error occurred
        if (hadError) {
            throw new Exception("Error in pre-processor.");
        }
        // Measure filtering time
        long filterTime = System.currentTimeMillis() - startMillis;
        PreStats.filterMillis = filterTime;

        // Create missing indices for columns involved in equi-joins.
        log("Creating indices ...");
        if (terminated) {
            PreStats.preMillis = System.currentTimeMillis() - startMillis;
            return preSummary;
        }
        long indexStart = System.currentTimeMillis();
        createJoinIndices(query, preSummary);
        long indexEnd = System.currentTimeMillis();

        // Measure processing time
        long indexTime = indexEnd - indexStart;
        PreStats.indexMillis = indexTime;
        System.out.println("Filter: " + filterTime + "\tIndex: " + indexTime);
        // construct mapping from join tables to index for each join predicate
        query.equiJoinPreds.forEach(expressionInfo -> {
            expressionInfo.extractIndex(preSummary);
            expressionInfo.setColumnType();
        });
        PreStats.preMillis = indexEnd - startMillis;
        return preSummary;
    }
    /**
     * Forms a conjunction between given conjuncts.
     *
     * @param conjuncts	list of conjuncts
     * @return	conjunction between all conjuncts or null
     * 			(iff the input list of conjuncts is empty)
     */
    static Expression conjunction(List<Expression> conjuncts) {
        Expression result = null;
        for (Expression conjunct : conjuncts) {
            if (result == null) {
                result = conjunct;
            } else {
                result = new AndExpression(
                        result, conjunct);
            }
        }
        return result;
    }

    /**
     * Check whether thee given predicate has some satisfied rows saved in the cache.
     *
     * @param curUnaryPred		current evaluating unary predicate.
     * @return					satisfied rows corresponding to given predicate.
     */
    static List<Integer> applyCache(ExpressionInfo curUnaryPred) {
        List<Integer> rows = BufferManager.indexCache.getOrDefault(curUnaryPred.pid, null);
        return rows;
    }
    /**
     * Search for applicable index and use it to prune rows. Redirect
     * column mappings to index-filtered table if possible.
     *
     * @param query			query to pre-process
     * @param unaryPred		unary predicate on that table
     * @param preSummary	summary of pre-processing steps
     * @return	remaining unary predicate to apply afterwards
     */
    static IndexFilter applyIndex(QueryInfo query, ExpressionInfo unaryPred,
                                  Context preSummary) throws Exception {
        log("Searching applicable index for " + unaryPred + " ...");
        // Divide predicate conjuncts depending on whether they can
        // be evaluated using indices alone.
        log("Conjuncts for " + unaryPred + ": " + unaryPred.conjuncts.toString());
        IndexTest indexTest = new IndexTest(query);
        List<Expression> indexedConjuncts = new ArrayList<>();
        List<Expression> nonIndexedConjuncts = new ArrayList<>();
        List<Expression> sortedConjuncts = new ArrayList<>();
        List<Expression> unsortedConjuncts = new ArrayList<>();
        Set<String> unsortedColumns = new HashSet<>();
        Set<String> sortedColumns = new HashSet<>();
        Set<String> indexedColumns = new HashSet<>();
        for (Expression conjunct : unaryPred.conjuncts) {
            // Re-initialize index test
            indexTest.canUseIndex = true;
            indexTest.constantQueue.clear();
            indexTest.columnNames.clear();
            indexTest.sorted = true;

            // Compare predicate against indexes
            conjunct.accept(indexTest);
            // Can conjunct be evaluated only from indices?
            if (indexTest.canUseIndex && PreConfig.CONSIDER_INDICES) {
                if (indexTest.sorted) {
                    sortedConjuncts.add(conjunct);
                    sortedColumns.addAll(indexTest.columnNames);
                }
                else {
                    unsortedConjuncts.add(conjunct);
                    unsortedColumns.addAll(indexTest.columnNames);
                }
            } else {
                nonIndexedConjuncts.add(conjunct);
            }
        }
        if (LoggingConfig.PREPROCESSING_VERBOSE) {
            log("Indexed:\t" + indexedConjuncts +
                    "; other: " + nonIndexedConjuncts);
        }
        // Create remaining predicate expression
        if (unsortedConjuncts.size() > 0) {
            indexedConjuncts.addAll(unsortedConjuncts);
            indexedColumns.addAll(unsortedColumns);
            nonIndexedConjuncts.addAll(sortedConjuncts);
        }
        else {
            indexedColumns.addAll(sortedColumns);
            indexedConjuncts.addAll(sortedConjuncts);
        }
        Expression remainingExpr = conjunction(nonIndexedConjuncts);
        // Evaluate indexed predicate part
        if (!indexedConjuncts.isEmpty()) {
            long indexStart = System.currentTimeMillis();
            IndexFilter indexFilter = new IndexFilter(query);
            indexFilter.isSameColumn = indexedColumns.size() == 1;
            Expression indexedExpr = conjunction(indexedConjuncts);
            indexedExpr.accept(indexFilter);
            long indexEnd = System.currentTimeMillis();
            // Create filtered table
            IntArrayList rows = indexFilter.qualifyingRows.peek();
            // Need to keep columns for evaluating remaining predicates, if any
            ExpressionInfo remainingInfo = null;
            String alias = unaryPred.aliasesMentioned.iterator().next();
            String table = query.aliasToTable.get(alias);
            Set<ColumnRef> colSuperset = new HashSet<>();
            colSuperset.addAll(query.colsForJoins);
            colSuperset.addAll(query.colsForPostProcessing);
            if (remainingExpr != null) {
                remainingInfo = new ExpressionInfo(query, remainingExpr);
                indexFilter.remainingInfo = remainingInfo;
                colSuperset.addAll(remainingInfo.columnsMentioned);
            }
            List<String> requiredCols = colSuperset.stream().
                    filter(c -> c.aliasName.equals(alias)).
                    map(c -> c.columnName).collect(Collectors.toList());
            String targetRelName = NamingConfig.IDX_FILTERED_PRE + alias;
            boolean needSort = false;
            int[] index_rows = new int[0];
            if (indexFilter.isFull) {
                index_rows = Arrays.copyOfRange(rows.elements(), 0, rows.size());
            }
            else if (!indexFilter.equalFull && rows != null && rows.size() == 1) {
                Materialize.executeEqualPos(table, requiredCols, rows,
                        indexFilter.lastIndex, targetRelName, true);
            }
            else {
                int[] sort = indexFilter.lastIndex.sortedRow;
                index_rows = Arrays.copyOfRange(sort, rows.getInt(0), rows.getInt(1));
                int nrIndexes = 0;
                for (String columnName: requiredCols) {
                    ColumnRef columnRef = new ColumnRef(alias, columnName);
                    if (query.indexCols.contains(columnRef)) {
                        nrIndexes++;
                    }
                }
                if (nrIndexes > 0) {
                    Arrays.parallelSort(index_rows);
                }
            }

            // All columns to be materialized
            List<int[]> intSrcCols = new ArrayList<>();
            List<double[]> doubleSrcCols = new ArrayList<>();
            List<ColumnRef> sourceColRefs = new ArrayList<>();
            List<ColumnRef> intColRefs = new ArrayList<>();
            List<ColumnRef> doubleColRefs = new ArrayList<>();
            for (String columnName: requiredCols) {
                ColumnRef columnRef = new ColumnRef(alias, columnName);
                ColumnRef mapRef = preSummary.columnMapping.get(columnRef);
                ColumnRef filteredRef = new ColumnRef(targetRelName, columnRef.columnName);
                sourceColRefs.add(mapRef);
                ColumnData columnData = BufferManager.getData(mapRef);
                if (columnData instanceof IntData) {
                    intSrcCols.add(((IntData) columnData).data);
                    intColRefs.add(filteredRef);
                }
                else {
                    doubleSrcCols.add(((DoubleData) columnData).data);
                    doubleColRefs.add(filteredRef);
                }
            }
            int intSize = intSrcCols.size() == 0 ? 0 : index_rows.length;
            int doubleSize = doubleSrcCols.size() == 0 ? 0 : index_rows.length;
            int[][] intTargetCols = new int[intSrcCols.size()][intSize];
            double[][] doubleTargetCols = new double[doubleSrcCols.size()][doubleSize];
            long materialStart = System.currentTimeMillis();
            JNIFilter.materialize(intSrcCols.toArray(new int[0][0]), doubleSrcCols.toArray(new double[0][0]),
                    intTargetCols, doubleTargetCols, index_rows, ParallelConfig.EXE_THREADS, needSort);
            long materialEnd = System.currentTimeMillis();
            // Materialize relevant rows and columns
            // Update catalog, inserting materialized table
            TableInfo resultTable = new TableInfo(targetRelName, true);
            CatalogManager.currentDB.addTable(resultTable);
            for (ColumnRef sourceColRef : sourceColRefs) {
                // Add result column to result table, using type of source column
                ColumnInfo sourceCol = CatalogManager.getColumn(sourceColRef);
                ColumnInfo resultCol = new ColumnInfo(sourceColRef.columnName,
                        sourceCol.type, sourceCol.isPrimary,
                        sourceCol.isUnique, sourceCol.isNotNull,
                        sourceCol.isForeign);
                resultTable.addColumn(resultCol);
            }
//			int newCardinality = intTargetCols.length > 0 ? intTargetCols[0].length : doubleTargetCols[0].length;
            for (int intColCtr = 0; intColCtr < intSrcCols.size(); intColCtr++) {
                IntData intData = new IntData(intTargetCols[intColCtr]);
                BufferManager.colToData.put(intColRefs.get(intColCtr), intData);
            }
            for (int doubleColCtr = 0; doubleColCtr < doubleSrcCols.size(); doubleColCtr++) {
                DoubleData doubleData = new DoubleData(doubleTargetCols[doubleColCtr]);
                BufferManager.colToData.put(doubleColRefs.get(doubleColCtr), doubleData);
            }
            System.out.println("Index Filter: " + (indexEnd - indexStart) +
                    "\tMaterialization: "
                    + (materialEnd - materialStart));


//            // Old version
//            if (indexFilter.isFull) {
//                Materialize.execute(table, requiredCols, rows,
//                        null, targetRelName, true);
//                System.out.println("After index applied: " + rows.size());
//            }
//            else if (!indexFilter.equalFull && rows != null && rows.size() == 1) {
//                Materialize.executeEqualPos(table, requiredCols, rows,
//                        indexFilter.lastIndex, targetRelName, true);
//            }
//            else {
//                int[] sort = indexFilter.lastIndex.sortedRow;
//                int nrIndexes = 0;
//                for (String columnName: requiredCols) {
//                    ColumnRef columnRef = new ColumnRef(alias, columnName);
//                    if (query.indexCols.contains(columnRef)) {
//                        nrIndexes++;
//                    }
//                }
//                if (nrIndexes > 0) {
//                    sort = Arrays.copyOfRange(indexFilter.lastIndex.sortedRow,
//                            rows.getInt(0), rows.getInt(1));
//                    Arrays.parallelSort(sort);
//                    rows.set(0, 0);
//                    rows.set(1, sort.length);
//                }
//                long materialStart = System.currentTimeMillis();
//                Materialize.executeRange(table, requiredCols, rows,
//                        sort, targetRelName, true);
//                long materialEnd = System.currentTimeMillis();
//                System.out.println("Index Filter: " + (indexEnd - indexStart) +
//                        "\tMaterialization: "
//                        + (materialEnd - materialStart));
//            }

            // Update pre-processing summary
            for (String colName : requiredCols) {
                ColumnRef queryRef = new ColumnRef(alias, colName);
                ColumnRef dbRef = new ColumnRef(targetRelName, colName);
                preSummary.columnMapping.put(queryRef, dbRef);
            }

            // Update statistics in catalog
            CatalogManager.updateStats(targetRelName);
            preSummary.aliasToFiltered.put(alias, targetRelName);
            return indexFilter;
        } else {
            IndexFilter indexFilter = new IndexFilter(query);
            indexFilter.lastIndex = null;
            indexFilter.remainingInfo = unaryPred;
            return indexFilter;
        }
    }
    /**
     * Creates a new temporary table containing remaining tuples
     * after applying unary predicates, project on columns that
     * are required for following steps.
     *
     * @param query			query to pre-process
     * @param alias			alias of table to filter
     * @param filter		unary predicate filter
     * @param requiredCols	project on those columns
     * @param preSummary	summary of pre-processing steps
     */
    static List<Integer> filterProject(QueryInfo query, String alias, IndexFilter filter,
                                       List<ColumnRef> requiredCols, Context preSummary) throws Exception {
        long startMillis = 0;
        ExpressionInfo unaryPred = filter.remainingInfo;
        if (LoggingConfig.PERFORMANCE_VERBOSE) {
            startMillis = System.currentTimeMillis();
            log("Filtering and projection for " + alias + " ...");
        }
        String tableName = preSummary.aliasToFiltered.get(alias);
        if (LoggingConfig.PERFORMANCE_VERBOSE) {
            log("Table name for " + alias + " is " + tableName);
        }
        // Determine rows satisfying unary predicate
        int[] indexFilteredRows = filter.rows == null ? new int[0] : filter.rows;
        int cardinality = indexFilteredRows.length == 0 ?
                CatalogManager.getCardinality(tableName) : indexFilteredRows.length;
        List<Integer> satisfyingRows;
        if (cardinality <= ParallelConfig.PRE_BATCH_SIZE) {
            satisfyingRows = Filter.executeToList(
                    filter, tableName, preSummary.columnMapping, query, requiredCols);
            // Materialize relevant rows and columns
            String filteredName = NamingConfig.FILTERED_PRE + alias;
            List<String> columnNames = new ArrayList<>();
            for (ColumnRef colRef : requiredCols) {
                columnNames.add(colRef.columnName);
            }
            long s2 = System.currentTimeMillis();
            Materialize.execute(tableName, columnNames,
                    satisfyingRows, null, filteredName, true);
            long s3 = System.currentTimeMillis();
            System.out.println("Materializing after filtering " + unaryPred + " took " + (s3 - s2));
            // Update pre-processing summary
            for (ColumnRef srcRef : requiredCols) {
                String columnName = srcRef.columnName;
                ColumnRef resRef = new ColumnRef(filteredName, columnName);
                preSummary.columnMapping.put(srcRef, resRef);
            }
            preSummary.aliasToFiltered.put(alias, filteredName);
        }
        else {
            satisfyingRows = Filter.executeToListJNI(
                    filter, tableName, preSummary, query, requiredCols, alias);
        }
        if (satisfyingRows == null) {
            List<Integer> returnedResults = new ArrayList<>();
            returnedResults.add(-1);
            return returnedResults;
        }
        if (LoggingConfig.PERFORMANCE_VERBOSE) {
            long totalMillis = System.currentTimeMillis() - startMillis;
            log("Filtering using " + unaryPred + " took " + totalMillis + " milliseconds");
        }
        return satisfyingRows;
    }
    /**
     * Create indices on equality join columns if not yet available.
     *
     * @param query			query for which to create indices
     * @param preSummary	summary of pre-processing steps executed so far
     * @throws Exception
     */
    static void createJoinIndices(QueryInfo query, Context preSummary)
            throws Exception {
        // Iterate over columns in equi-joins
        long startMillis = System.currentTimeMillis();

        query.indexCols.forEach(queryRef -> {
            try {
                // Resolve query-specific column reference
                ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
                if (LoggingConfig.PREPROCESSING_VERBOSE) {
                    log("Creating index for " + queryRef +
                            " (query) - " + dbRef + " (DB)");
                }
                long timer1 = System.currentTimeMillis();
                ColumnInfo columnInfo = query.colRefToInfo.get(queryRef);
                String tableName = query.aliasToTable.get(queryRef.aliasName);
                String columnName = queryRef.columnName;
                ColumnRef columnRef = new ColumnRef(tableName, columnName);
                Index index = BufferManager.colToIndex.getOrDefault(columnRef, null);
                PartitionIndex partitionIndex = index == null ? null : (PartitionIndex) index;
                // Get index generation policy according to statistics.
                // Create index (unless it exists already)
                Indexer.partitionIndex(query.aliasToPositions.get(columnRef), columnRef,
                        dbRef, queryRef, partitionIndex, columnInfo.isPrimary);
                long timer2 = System.currentTimeMillis();
                System.out.println("Indexing " + queryRef + " " + (timer2 - timer1));
            } catch (Exception e) {
                System.err.println("Error creating index for " + queryRef);
                e.printStackTrace();
            }
        });
        long totalMillis = System.currentTimeMillis() - startMillis;
        System.out.println("Created all indices in " + totalMillis + " ms.");
        log("Created all indices in " + totalMillis + " ms.");
    }
    /**
     * Output logging message if pre-processing logging activated.
     *
     * @param toLog		text to display if logging is activated
     */
    static void log(String toLog) {
        if (LoggingConfig.PREPROCESSING_VERBOSE) {
            System.out.println(toLog);
        }
    }
}