package preprocessing;

import java.util.*;
import java.util.stream.Collectors;


import buffer.BufferManager;
import catalog.info.ColumnInfo;
import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import indexing.Index;
import indexing.Indexer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import operators.Filter;
import operators.IndexFilter;
import operators.IndexTest;
import operators.Materialize;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.PreStats;

/**
 * Filters query tables via unary predicates and stores
 * result in newly created tables. Creates hash tables
 * for columns with binary equality join predicates.
 * 
 * @author immanueltrummer
 *
 */
public class Preprocessor {
	/**
	 * Whether an error occurred during last invocation.
	 * This flag is used in cases where an error occurs
	 * without an exception being thrown.
	 */
	public static boolean hadError = false;
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
		// Reset error flag
		hadError = false;
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
		log("Column mapping:\t" + preSummary.columnMapping.toString());
		// Iterate over query aliases
		long timer00 = System.currentTimeMillis();
		query.aliasToTable.keySet().parallelStream().forEach(alias -> {
//			long timer0 = System.currentTimeMillis();
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
			// Filter and project if enabled
			if (curUnaryPred != null && PreConfig.PRE_FILTER) {
				try {
					//check if the predicate is in the cache
					List<Integer> inCacheRows = applyCache(curUnaryPred, preSummary);
					if (!PreConfig.IN_CACHE || inCacheRows == null) {
						// Apply index to prune rows if possible
						ExpressionInfo remainingPred = applyIndex(
								query, curUnaryPred, preSummary);
						// TODO: reinsert index usage
						//ExpressionInfo remainingPred = curUnaryPred;
						// Filter remaining rows by remaining predicate
						if (remainingPred != null) {
							List<Integer> rows = filterProject(query, alias, remainingPred,
									curRequiredCols, preSummary);
							if (rows.size() > 0) {
								BufferManager.indexCache.putIfAbsent(curUnaryPred.pid, rows);
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
						Materialize.execute(tableName, columnNames,
								inCacheRows, null, filteredName);
						// Update pre-processing summary
						for (ColumnRef srcRef : curRequiredCols) {
							String columnName = srcRef.columnName;
							ColumnRef resRef = new ColumnRef(filteredName, columnName);
							preSummary.columnMapping.put(srcRef, resRef);
						}
						preSummary.aliasToFiltered.put(alias, filteredName);
						log("Cache hit using " + curUnaryPred);

					}
				} catch (Exception e) {
					System.err.println("Error filtering " + alias);
					e.printStackTrace();
					hadError = true;
				}
			} else {
//				long timer1 = System.currentTimeMillis();
				String table = query.aliasToTable.get(alias);
				preSummary.aliasToFiltered.put(alias, table);
//				System.out.println(alias + ": " + (timer1 - timer0));
			}
		});
		long totalMillis = System.currentTimeMillis() - timer00;
		System.out.println("Filtering: " + totalMillis + " ms.");
		PreStats.filterTime = totalMillis;
		// Abort pre-processing if filtering error occurred
		if (hadError) {
			throw new Exception("Error in pre-processor.");
		}
		// Create missing indices for columns involved in equi-joins.
		log("Creating indices ...");			
		createJoinIndices(query, preSummary);
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

	static List<Integer> applyCache(ExpressionInfo curUnaryPred, Context preSummary) {
		List<Integer> rows = BufferManager.indexCache.getOrDefault(curUnaryPred.pid, null);
		return rows;
//		if (cachedIndices != null) {
//			String alias = curUnaryPred.aliasesMentioned.iterator().next();
//			String cachedName = NamingConfig.CACHED_FILTERED_PRE + alias;
//			// Update pre-processing summary
//
//			for (ThreadIntIndex index: cachedIndices) {
//				ColumnRef srcRef = index.queryRef;
//				String columnName = srcRef.columnName;
//				ColumnRef resRef = new ColumnRef(cachedName, columnName);
//
//				preSummary.columnMapping.put(srcRef, resRef);
//
//				BufferManager.colToIndex.put(resRef, index);
//			}
//			preSummary.aliasToFiltered.put(alias, cachedName);
//			return true;
//		}
//		return false;
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
	static ExpressionInfo applyIndex(QueryInfo query, ExpressionInfo unaryPred,
			Context preSummary) throws Exception {
		log("Searching applicable index for " + unaryPred + " ...");
		// Divide predicate conjuncts depending on whether they can
		// be evaluated using indices alone.
		log("Conjuncts for " + unaryPred + ": " + unaryPred.conjuncts.toString());
		IndexTest indexTest = new IndexTest(query);
		List<Expression> indexedConjuncts = new ArrayList<>();
		List<Expression> nonIndexedConjuncts = new ArrayList<>();
		for (Expression conjunct : unaryPred.conjuncts) {
			// Re-initialize index test
			indexTest.canUseIndex = true;
			// Compare predicate against indexes
			conjunct.accept(indexTest);
			// Can conjunct be evaluated only from indices?
			if (indexTest.canUseIndex) {
				indexedConjuncts.add(conjunct);
			} else {
				nonIndexedConjuncts.add(conjunct);
			}
		}
		log("Indexed:\t" + indexedConjuncts.toString() + 
				"; other: " + nonIndexedConjuncts.toString());
		// Create remaining predicate expression
		Expression remainingExpr = conjunction(nonIndexedConjuncts);
		// Evaluate indexed predicate part
		if (!indexedConjuncts.isEmpty()) {
			IndexFilter indexFilter = new IndexFilter(query);
			Expression indexedExpr = conjunction(indexedConjuncts);
			indexedExpr.accept(indexFilter);
			List<Integer> rows = indexFilter.qualifyingRows.pop();
			// Create filtered table
			String alias = unaryPred.aliasesMentioned.iterator().next();
			String table = query.aliasToTable.get(alias);
			Set<ColumnRef> colSuperset = new HashSet<>();
			colSuperset.addAll(query.colsForJoins);
			colSuperset.addAll(query.colsForPostProcessing);
			// Need to keep columns for evaluating remaining predicates, if any
			ExpressionInfo remainingInfo = null;
			if (remainingExpr != null) {
				remainingInfo = new ExpressionInfo(query, remainingExpr);
				colSuperset.addAll(remainingInfo.columnsMentioned);				
			}
			List<String> requiredCols = colSuperset.stream().
					filter(c -> c.aliasName.equals(alias)).
					map(c -> c.columnName).collect(Collectors.toList());
			String targetRelName = NamingConfig.IDX_FILTERED_PRE + alias;
			Materialize.execute(table, requiredCols, rows, null, targetRelName);
			// Update pre-processing summary
			for (String colName : requiredCols) {
				ColumnRef queryRef = new ColumnRef(alias, colName);
				ColumnRef dbRef = new ColumnRef(targetRelName, colName);
				preSummary.columnMapping.put(queryRef, dbRef);
			}
			preSummary.aliasToFiltered.put(alias, targetRelName);
			log(preSummary.toString());
			return remainingInfo;
		} else {
			return unaryPred;
		}
	}
	/**
	 * Creates a new temporary table containing remaining tuples
	 * after applying unary predicates, project on columns that
	 * are required for following steps.
	 * 
	 * @param query			query to pre-process
	 * @param alias			alias of table to filter
	 * @param unaryPred		unary predicate on that table
	 * @param requiredCols	project on those columns
	 * @param preSummary	summary of pre-processing steps
	 */
	static List<Integer> filterProject(QueryInfo query, String alias, ExpressionInfo unaryPred,
			List<ColumnRef> requiredCols, Context preSummary) throws Exception {
		long startMillis = System.currentTimeMillis();
		log("Filtering and projection for " + alias + " ...");
		String tableName = preSummary.aliasToFiltered.get(alias);
		log("Table name for " + alias + " is " + tableName);
		// Determine rows satisfying unary predicate
		List<Integer> satisfyingRows = Filter.executeToList(
				unaryPred, tableName, preSummary.columnMapping);
		long timer0 = System.currentTimeMillis();
		// Materialize relevant rows and columns
		String filteredName = NamingConfig.FILTERED_PRE + alias;
		List<String> columnNames = new ArrayList<>();
		for (ColumnRef colRef : requiredCols) {
			columnNames.add(colRef.columnName);
		}
		long timer1 = System.currentTimeMillis();
		Materialize.execute(tableName, columnNames,
				satisfyingRows, null, filteredName);
		long timer2 = System.currentTimeMillis();
		// Update pre-processing summary
		for (ColumnRef srcRef : requiredCols) {
			String columnName = srcRef.columnName;
			ColumnRef resRef = new ColumnRef(filteredName, columnName);
			preSummary.columnMapping.put(srcRef, resRef);
		}
		preSummary.aliasToFiltered.put(alias, filteredName);
		long timer3 = System.currentTimeMillis();
		long totalMillis = System.currentTimeMillis() - startMillis;
		log("Filtering using " + unaryPred + " took " + totalMillis + " milliseconds");
		// Print out intermediate result table if logging is enabled
		if (LoggingConfig.PRINT_INTERMEDIATES) {
			RelationPrinter.print(filteredName);
		}
		System.out.println("Filtering using " + unaryPred + " took: " + (timer0 - startMillis) + "\t" + (timer1 - timer0) + "\t" + (timer2 - timer1) + "\t" + (timer3 - timer2));
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
//		for (ColumnRef queryRef: query.equiJoinCols) {
//			long t0 = System.currentTimeMillis();
//			ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
//			Indexer.index(dbRef);
//			long t1 = System.currentTimeMillis();
//			System.out.println("Creating index for " + queryRef +
//					" (query) - " + dbRef + " (DB)" + " in " + (t1 - t0) + " ms");
//		}


		query.equiJoinCols.parallelStream().forEach(queryRef -> {
			try {
				// Resolve query-specific column reference
				ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
				ColumnInfo columnInfo = query.colRefToInfo.get(queryRef);
				String tableName = query.aliasToTable.get(queryRef.aliasName);
				String columnName = queryRef.columnName;
				ColumnRef columnRef = new ColumnRef(tableName, columnName);
				Index index = BufferManager.colToIndex.getOrDefault(columnRef, null);

				log("Creating index for " + queryRef +
						" (query) - " + dbRef + " (DB)");
				// Create index (unless it exists already)
				Indexer.index(dbRef, queryRef, index, columnInfo.isPrimary);
			} catch (Exception e) {
				System.err.println("Error creating index for " + queryRef);
				e.printStackTrace();
			}
		});

//		query.aliasToExpression.keySet().parallelStream().forEach(alias -> {
//
//		});
//
//		query.unaryPredicates.parallelStream().forEach(predicate -> {
//			Set<String> aliasesMentioned = predicate.aliasesMentioned;
//			String alias = aliasesMentioned.iterator().next();
//			query.al
//			aliasesMentioned.parallelStream().forEach(alias -> {
//				String columnName =
//			});
//		});


		long totalMillis = System.currentTimeMillis() - startMillis;
		PreStats.indexTime = totalMillis;
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