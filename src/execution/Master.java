package execution;

import joining.JoinProcessor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.QueryInfo;

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
		// Analyze input query
		QueryInfo query = new QueryInfo(select, explain, 
				plotAtMost, plotEvery, plotDir);
		// Filter, projection, and indexing for join phase
		Context context = Preprocessor.process(query);
		// Join filtered tables
		JoinProcessor.process(query, context);
		// Aggregation, grouping, and sorting if required
		PostProcessor.process(query, context);
	}
}
