package execution;

import java.util.*;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.*;
import expressions.VisitorUtil;
import joining.JoinProcessor;
import joining.ParallelJoinProcessor;
import joining.result.ResultTuple;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import operators.Materialize;
import postprocessing.ParallelPostProcessor;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;
import unnesting.UnnestingVisitor;

/**
 * Controls high-level execution process.
 * 
 * @author immanueltrummer
 *
 */
public class Master {
	/**
	 * Executes given (plain) select statement and
	 * writes query result into new relation with
	 * specified name.
	 * 
	 * @param select		select statement to execute
	 * @param explain		whether to explain query execution via visualizations
	 * @param plotAtMost	generate at most that many plots if activated
	 * @param plotEvery		generate one plot after X samples if activated
	 * @param plotDir		add plots to this directory if activated
	 * @throws Exception
	 */
	public static void executeSelect(PlainSelect select, 
			boolean explain, int plotAtMost, int plotEvery,
			String plotDir) throws Exception {
		// initialize statistics variables
		initializeStats();
		// Determine type of result relation
		List<Table> intoTbls = select.getIntoTables();
		boolean finalTempResult = intoTbls == null;
		// Ensure single target relation if any
		if (intoTbls != null && intoTbls.size()>1) {
			throw new SQLexception("Error - at most one"
					+ "relation to copy into is allowed");
		}
		// Add default result relation if required
		if (intoTbls == null) {
			intoTbls = new ArrayList<>();
			Table intoTable = new Table(NamingConfig.FINAL_RESULT_NAME);
			intoTbls.add(intoTable);
			select.setIntoTables(intoTbls);
		}
		// Unnest input query
		UnnestingVisitor unnestor = new UnnestingVisitor();
		VisitorUtil.tryVisit(select, unnestor);
		if (LoggingConfig.UNNESTING_VERBOSE) {
			System.out.println("Sub-query sequence generated during unnesting: ");
			System.out.println(unnestor.unnestedQueries);
		}
		// Process sub-queries in order
		Set<String> subQueryResults = new HashSet<>();
		int nrSubQueries = unnestor.unnestedQueries.size();
		for (int subQueryCtr=0; subQueryCtr<nrSubQueries; ++subQueryCtr) {
			// Retrieve next sub-query
			PlainSelect subQuery = unnestor.unnestedQueries.get(subQueryCtr);
			// Analyze sub-query
			QueryInfo subQueryInfo = new QueryInfo(subQuery, explain, 
					plotAtMost, plotEvery, plotDir);
			PreConfig.FILTER = PreConfig.PRE_FILTER;
			// Filter, projection, and indexing for join phase
			Preprocessor.performance = true;
			Context context = Preprocessor.process(subQueryInfo);
			if (Preprocessor.terminated) {
				JoinStats.exeTime = 0;
				JoinStats.subExeTime.add(JoinStats.exeTime);
				PostStats.postMillis = 0;
				PostStats.subPostMillis.add(PostStats.postMillis);
				String targetRelName = NamingConfig.JOINED_NAME;
				Materialize.execute(new HashSet<>(), subQueryInfo.aliasToIndex,
						subQueryInfo.colsForPostProcessing,
						context.columnMapping, targetRelName);
//            // Update processing context
				context.columnMapping.clear();
				for (ColumnRef postCol : subQueryInfo.colsForPostProcessing) {
					String newColName = postCol.aliasName + "." + postCol.columnName;
					ColumnRef newRef = new ColumnRef(targetRelName, newColName);
					context.columnMapping.put(postCol, newRef);
				}
				JoinStats.subMateriazed.add(0L);
				// Store number of join result tuples
				JoinStats.skinnerJoinCards.add(0);
				break;
			}
			// Join filtered tables
//			if (GeneralConfig.isParallel) {
//				// Convert nonEqui-predicates into nodes
//				subQueryInfo.convertNonEquiPredicates(context);
//				ParallelJoinProcessor.process(subQueryInfo, context);
//			}
//			else {
//				JoinProcessor.process(subQueryInfo, context);
//			}
			// Convert nonEqui-predicates into nodes
			subQueryInfo.convertNonEquiPredicates(context);
			ParallelJoinProcessor.process(subQueryInfo, context);

			// Determine result table name and properties
			boolean lastSubQuery = subQueryCtr==nrSubQueries-1;
			boolean tempResult = lastSubQuery?finalTempResult:true;
			String resultRel = subQuery.getIntoTables().get(0).getName();
			// Aggregation, grouping, and sorting if required
			if (GeneralConfig.isParallel) {
				ParallelPostProcessor.process(subQueryInfo, context,
						resultRel, tempResult);
			}
			else {
				PostProcessor.process(subQueryInfo, context,
						resultRel, tempResult);
			}
			System.out.println(Arrays.toString(PostStats.subPostMillis.toArray()));
//			RelationPrinter.print(resultRel);
			// Clean up intermediate results except result table
			subQueryResults.add(resultRel);
			BufferManager.unloadTempData(subQueryResults);
			CatalogManager.removeTempTables(subQueryResults);
		}
	}

	private static void initializeStats() {
		PreStats.subPreMillis = new ArrayList<>();
		JoinStats.skinnerJoinCards = new ArrayList<>();
		JoinStats.subJoinTime = new ArrayList<>();
		JoinStats.subMateriazed = new ArrayList<>();
		JoinStats.subExeTime = new ArrayList<>();
		PostStats.subPostMillis = new ArrayList<>();
	}
}
