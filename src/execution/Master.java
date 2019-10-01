package execution;


import buffer.BufferManager;
import catalog.CatalogManager;
import joining.ParallelJoinProcessor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.QueryInfo;
import statistics.JoinStats;

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
	 * @param targetRel	name of result table to create
	 * @throws Exception
	 */
	public static void executeSelect(PlainSelect select, 
			boolean explain, int plotAtMost, int plotEvery,
			String plotDir, String targetRel) throws Exception {
        executeStart(select,
                false, -1, -1, null);
		// Analyze input query
		QueryInfo query = new QueryInfo(select, explain, 
				plotAtMost, plotEvery, plotDir);
		long preStart = System.currentTimeMillis();
		// Filter, projection, and indexing for join phase
		Context context = Preprocessor.process(query);
		System.out.println("Finish Pre");
		long joinStart = System.currentTimeMillis();
		query.equiJoinPreds.forEach(expressionInfo -> {
			expressionInfo.extractIndex(context);
		});
		// Join filtered tables
//		JoinProcessor.process(query, context);
		ParallelJoinProcessor.process(query, context);
		System.out.println("Finish Join");
		long postStart = System.currentTimeMillis();
		// Aggregation, grouping, and sorting if required
		PostProcessor.process(query, context);
//		ParallelPostProcessor.process(query, context);
		System.out.println("Finish Post");
		long end = System.currentTimeMillis();
		System.out.println("Pre: " + (joinStart - preStart) + "\tJoin: " +
				(postStart - joinStart) + "\tExe: " + JoinStats.time + "\tPost: " + (end - postStart));
	}

	public static void executeStart(PlainSelect select,
									 boolean explain, int plotAtMost, int plotEvery,
									 String plotDir) throws Exception {
		// Analyze input query
		QueryInfo query = new QueryInfo(select, explain,
				plotAtMost, plotEvery, plotDir);
		// Filter, projection, and indexing for join phase
		Context context = Preprocessor.process(query);
		query.equiJoinPreds.forEach(expressionInfo -> expressionInfo.extractIndex(context));
		// Join filtered tables
		ParallelJoinProcessor.process(query, context);
		// Aggregation, grouping, and sorting if required
		PostProcessor.process(query, context);
		BufferManager.unloadTempData();
		CatalogManager.removeTempTables();
	}


}
