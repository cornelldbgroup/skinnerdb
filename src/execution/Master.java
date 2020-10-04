package execution;

import java.io.PrintWriter;
import java.util.*;

import benchmark.BenchUtil;
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
import preprocessing.NewPreprocessor;
import preprocessing.Preprocessor;
import print.RelationPrinter;
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
	 * @param queryName		name of query to use for benchmark output
	 * @param benchOut		writer to benchmark file, null to deactivate
	 * @throws Exception
	 */
	public static void executeSelect(PlainSelect select, 
			boolean explain, int plotAtMost, int plotEvery,
			String plotDir, String queryName, PrintWriter benchOut) throws Exception {
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
			long startMillis = System.currentTimeMillis();
			// Retrieve next sub-query
			PlainSelect subQuery = unnestor.unnestedQueries.get(subQueryCtr);
			Set<String> temporary = unnestor.temporaryTables.get(subQueryCtr);
			// Analyze sub-query
			QueryInfo subQueryInfo = new QueryInfo(subQuery, temporary, explain,
					plotAtMost, plotEvery, plotDir);
			PreConfig.FILTER = PreConfig.PRE_FILTER;
			// Filter, projection, and indexing for join phase
			Preprocessor.performance = true;
			Context context = Preprocessor.process(subQueryInfo);
			if (Preprocessor.terminated) {
				JoinStats.exeTime = 0;
				JoinStats.joinMillis = 0;
				PostStats.postMillis = 0;
				PostStats.havingMillis = 0;
				PostStats.aggMillis = 0;
				PostStats.groupByMillis = 0;
				PostStats.orderMillis = 0;

				String targetRelName = NamingConfig.JOINED_NAME;
				Materialize.execute(new HashSet<>(), subQueryInfo.aliasToIndex,
						subQueryInfo.colsForPostProcessing,
						context.columnMapping, targetRelName);
				// Update processing context
				context.columnMapping.clear();
				for (ColumnRef postCol : subQueryInfo.colsForPostProcessing) {
					String newColName = postCol.aliasName + "." + postCol.columnName;
					ColumnRef newRef = new ColumnRef(targetRelName, newColName);
					context.columnMapping.put(postCol, newRef);
				}
				break;
			}

			// Convert nonEqui-predicates into nodes
			subQueryInfo.convertNonEquiPredicates(context);
			// Extract selectivity
			subQueryInfo.extractSelectivity(context);
			ParallelJoinProcessor.process(subQueryInfo, context);

			// Determine result table name and properties
			boolean lastSubQuery = subQueryCtr==nrSubQueries-1;
			boolean tempResult = !lastSubQuery || finalTempResult;
			String resultRel = subQuery.getIntoTables().get(0).getName();
			if (!CatalogManager.currentDB.nameToTable.containsKey(resultRel)) {
				// Aggregation, grouping, and sorting if required
				PostProcessor.process(subQueryInfo, context,
						resultRel, tempResult);
				if (StartupConfig.Memory) {
					JoinStats.dataSize = BufferManager.getTempDataSize(subQueryResults);
				}
			}
			// Generate benchmark output if activated
			if (benchOut != null) {
				long totalMillis = System.currentTimeMillis() - startMillis;
				BenchUtil.writeStats(queryName, totalMillis, benchOut);
			}
			// Clean up intermediate results except result table
			subQueryResults.add(resultRel);
			BufferManager.unloadTempData(subQueryResults);
			CatalogManager.removeTempTables(subQueryResults);
		}
	}

	private static void initializeStats() {
		PreStats.preMillis = 0;
		JoinStats.joinMillis = 0;
		JoinStats.exeTime = 0;
		JoinStats.mergeTime = 0;
		JoinStats.matMillis = 0;
		PostStats.postMillis = 0;

		PreStats.filterMillis = 0;
		PreStats.indexMillis = 0;
		PostStats.groupByMillis = 0;
		PostStats.aggMillis = 0;
		PostStats.havingMillis = 0;
		PostStats.orderMillis = 0;

		JoinStats.nrTuples = 0;
		JoinStats.nrSamples = 0;
		JoinStats.nrIndexLookups = 0;
		JoinStats.nrIndexEntries = 0;
		JoinStats.nrUniqueIndexLookups = 0;
		JoinStats.nrUctNodes = 0;
		JoinStats.nrPlansTried = 0;
		JoinStats.lastJoinCard = 0;
		JoinStats.avgReward = 0;
		JoinStats.maxReward = 0;
		JoinStats.totalWork = 0;

		JoinStats.dataSize = 0;
		JoinStats.treeSize = 0;
		JoinStats.stateSize = 0;
		JoinStats.joinSize = 0;
	}
}
