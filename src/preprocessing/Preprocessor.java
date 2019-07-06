package preprocessing;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import buffer.BufferManager;
import config.LoggingConfig;
import config.NamingConfig;
import config.PreConfig;
import expressions.ExpressionInfo;
import indexing.Index;
import indexing.Indexer;
import indexing.IntIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import operators.Filter;
import operators.IndexFilter;
import operators.IndexTest;
import operators.Materialize;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;

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
		Set<ColumnRef> requiredCols = new HashSet<ColumnRef>();
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
		query.aliasToTable.keySet().parallelStream().forEach(alias -> {
			// Collect required columns (for joins and post-processing) for this table
			List<ColumnRef> curRequiredCols = new ArrayList<ColumnRef>();
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
					// Apply index to prune rows if possible
					ExpressionInfo remainingPred = applyIndex(
							query, curUnaryPred, preSummary);
					// TODO: reinsert index usage
					//ExpressionInfo remainingPred = curUnaryPred;
					// Filter remaining rows by remaining predicate
					if (remainingPred != null) {
						filterProject(query, alias, remainingPred,
								curRequiredCols, preSummary);						
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
		});			
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
	static void filterProject(QueryInfo query, String alias, ExpressionInfo unaryPred, 
			List<ColumnRef> requiredCols, Context preSummary) throws Exception {
		long startMillis = System.currentTimeMillis();
		log("Filtering and projection for " + alias + " ...");
		String tableName = preSummary.aliasToFiltered.get(alias);
		log("Table name for " + alias + " is " + tableName);
		// Determine rows satisfying unary predicate
		List<Integer> satisfyingRows = Filter.executeToList(
				unaryPred, tableName, preSummary.columnMapping);
		// Materialize relevant rows and columns
		String filteredName = NamingConfig.FILTERED_PRE + alias;
		List<String> columnNames = new ArrayList<String>();
		for (ColumnRef colRef : requiredCols) {
			columnNames.add(colRef.columnName);
		}
		Materialize.execute(tableName, columnNames, 
				satisfyingRows, null, filteredName);
		// Update pre-processing summary
		for (ColumnRef srcRef : requiredCols) {
			String columnName = srcRef.columnName;
			ColumnRef resRef = new ColumnRef(filteredName, columnName);
			preSummary.columnMapping.put(srcRef, resRef);
		}
		preSummary.aliasToFiltered.put(alias, filteredName);
		long totalMillis = System.currentTimeMillis() - startMillis;
		log("Filtering using " + unaryPred + " took " + totalMillis + " milliseconds");
		// Print out intermediate result table if logging is enabled
		if (LoggingConfig.PRINT_INTERMEDIATES) {
			RelationPrinter.print(filteredName);
		}
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
		query.equiJoinCols.parallelStream().forEach(queryRef -> {
			try {
				// Resolve query-specific column reference
				ColumnRef dbRef = preSummary.columnMapping.get(queryRef);
				log("Creating index for " + queryRef + 
						" (query) - " + dbRef + " (DB)");
				// Create index (unless it exists already)
				Indexer.index(dbRef);
			} catch (Exception e) {
				System.err.println("Error creating index for " + queryRef);
				e.printStackTrace();
			}
		});
		long totalMillis = System.currentTimeMillis() - startMillis;
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