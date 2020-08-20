package benchmark;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import catalog.CatalogManager;
import config.NamingConfig;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

/**
 * Several auxiliary methods for benchmarking SkinnerDB.
 * 
 * @author immanueltrummer
 *
 */
public class BenchUtil {
	/**
	 * Delimiter used for separating fields in benchmark results.
	 */
	static String delimiter = ",";
	/**
	 * Parses queries in all '.sql' files that are found
	 * in given directory and returns mapping from file
	 * names to queries.
	 * 
	 * @param dirPath	path to directory to read queries from
	 * @return			ordered mapping from file names to queries
	 * @throws Exception
	 */
	public static Map<String, PlainSelect> readAllQueries(
			String dirPath) throws Exception {
		Map<String, PlainSelect> nameToQuery =
				/*
				new TreeMap<String, PlainSelect>(
						Collections.reverseOrder());
				*/
				new TreeMap<String, PlainSelect>();
		File dir = new File(dirPath);
		for (File file : dir.listFiles()) {
			if (file.getName().endsWith(".sql")) {
				String sql = new String(Files.readAllBytes(file.toPath()));
				System.out.println(sql);
				Statement sqlStatement = CCJSqlParserUtil.parse(sql);
				Select select = (Select)sqlStatement;
				PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
				nameToQuery.put(file.getName(), plainSelect);				
			}
		}
		return nameToQuery;
	}
	/**
	 * Writes header row of benchmark result file.
	 * 
	 * @param benchOut	channel to benchmark file
	 */
	public static void writeBenchHeader(PrintWriter benchOut) {
		benchOut.println("Query" + delimiter + "Millis" + 
				delimiter + "PreMillis" + delimiter + "PostMillis" + 
				delimiter + "Tuples" + delimiter + "Iterations" + 
				delimiter + "Lookups" + delimiter + "NrIndexEntries" + 
				delimiter + "nrUniqueLookups" + 
				delimiter + "NrUctNodes" + 
				delimiter + "NrPlans" + 
				delimiter + "JoinCard" + 
				delimiter + "NrSamples" + 
				delimiter + "AvgReward" + 
				delimiter + "MaxReward" + 
				delimiter + "TotalWork");
	}
	/**
	 * Writes out statistics concerning last query execution
	 * into given benchmark result file.
	 * 
	 * @param queryName		name of query to process
	 * @param totalMillis	total milliseconds of query execution
	 * @param benchOut		channel for benchmark results
	 * @throws Exception
	 */
	public static void writeStats(String queryName, long totalMillis, 
			PrintWriter benchOut) throws Exception {
		// Get cardinality of Skinner join result
		int skinnerJoinCard = CatalogManager.getCardinality(
				NamingConfig.JOINED_NAME);
		// Generate output
		benchOut.print(queryName + delimiter);
		benchOut.print(totalMillis + delimiter);
		benchOut.print(PreStats.preMillis + delimiter);
		benchOut.print(PostStats.postMillis + delimiter);
		benchOut.print(JoinStats.nrTuples + delimiter);
		benchOut.print(JoinStats.nrIterations + delimiter);
		benchOut.print(JoinStats.nrIndexLookups + delimiter);
		benchOut.print(JoinStats.nrIndexEntries + delimiter);
		benchOut.print(JoinStats.nrUniqueIndexLookups + delimiter);
		benchOut.print(JoinStats.nrUctNodes + delimiter);
		benchOut.print(JoinStats.nrPlansTried + delimiter);
		benchOut.print(skinnerJoinCard + delimiter);
		benchOut.print(JoinStats.nrSamples + delimiter);
		benchOut.print(JoinStats.avgReward + delimiter);
		benchOut.print(JoinStats.maxReward + delimiter);
		benchOut.println(JoinStats.totalWork);
		benchOut.flush();
	}
}
