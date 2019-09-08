package execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.LoggingConfig;
import config.NamingConfig;
import expressions.VisitorUtil;
import joining.JoinProcessor;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.ColumnRef;
import query.QueryInfo;
import query.SQLexception;
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
			intoTbls = new ArrayList<Table>();
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
			// Filter, projection, and indexing for join phase
			Context context = Preprocessor.process(subQueryInfo);
			// Join filtered tables
			JoinProcessor.process(subQueryInfo, context);
			// Determine result table name and properties
			boolean lastSubQuery = subQueryCtr==nrSubQueries-1;
			boolean tempResult = lastSubQuery?finalTempResult:true;
			String resultRel = subQuery.getIntoTables().get(0).getName();
			// Aggregation, grouping, and sorting if required
			PostProcessor.process(subQueryInfo, context, 
					resultRel, tempResult);
			// Clean up intermediate results except result table
			subQueryResults.add(resultRel);
			BufferManager.unloadTempData(subQueryResults);
			CatalogManager.removeTempTables(subQueryResults);
		}
	}
}
