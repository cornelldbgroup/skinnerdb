package benchmark;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Pattern;

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
	 * Parses queries in all '.sql' files that are found
	 * in given directory and returns mapping from file
	 * names to queries.
	 * 
	 * @param dirPath	path to directory to read queries from
	 * @return			ordered mapping from file names to queries
	 * @throws Exception
	 */
	public static Map<String, Statement> readAllQueries(
			String dirPath) throws Exception {
		Map<String, Statement> nameToQuery =
				new TreeMap<>(
						Collections.reverseOrder());
		File dir = new File(dirPath);
		for (File file : dir.listFiles()) {
			Scanner scanner = new Scanner(file);
			scanner.useDelimiter(Pattern.compile(";"));
			
			if (file.getName().endsWith(".sql")) {
				int id = 0;
				while (scanner.hasNext()) {
					String sqlCmd = scanner.next().trim();
					if (!sqlCmd.equals("")) {
						System.out.println(sqlCmd);
						String queryName = file.getName().split(".sql")[0] + (char)('a' - id) + ".sql";
						id++;
						try {
							Statement sqlStatement = CCJSqlParserUtil.parse(sqlCmd);
//							Select select = (Select)sqlStatement;
//							PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
							nameToQuery.put(queryName, sqlStatement);
						} catch (Exception e) {
							throw new Exception("query parse error!");
						}
					}
				}
			}
			scanner.close();
		}
		return nameToQuery;
	}
	/**
	 * Writes header row of benchmark result file.
	 * 
	 * @param benchOut	channel to benchmark file
	 */
	public static void writeBenchHeader(PrintWriter benchOut) {
		benchOut.println("Query\tMillis\tPreMillis\tJoinMillis\tExeMillis\tPostMillis\tTuples\t"
				+ "Iterations\tLookups\tNrIndexEntries\tnrUniqueLookups\t"
				+ "NrUctNodes\tNrPlans\tJoinCard\tNrSamples\tAvgReward\t"
				+ "MaxReward\tTotalWork\tFilterMillis\tIndexMillis\tSubPre\tSubJoin\tSubMaterial\tSubPost");
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
		// Generate output
		benchOut.print(queryName + "\t");
		benchOut.print(totalMillis + "\t");
		benchOut.print(PreStats.preMillis + "\t");
		benchOut.print(JoinStats.joinMillis + "\t");
		benchOut.print(JoinStats.exeTime + "\t");
		benchOut.print(PostStats.postMillis + "\t");
		benchOut.print(JoinStats.nrTuples + "\t");
		benchOut.print(JoinStats.nrIterations + "\t");
		benchOut.print(JoinStats.nrIndexLookups + "\t");
		benchOut.print(JoinStats.nrIndexEntries + "\t");
		benchOut.print(JoinStats.nrUniqueIndexLookups + "\t");
		benchOut.print(JoinStats.nrUctNodes + "\t");
		benchOut.print(JoinStats.nrPlansTried + "\t");
		benchOut.print(JoinStats.skinnerJoinCards + "\t");
		benchOut.print(JoinStats.nrSamples + "\t");
		benchOut.print(JoinStats.avgReward + "\t");
		benchOut.print(JoinStats.maxReward + "\t");
		benchOut.print(JoinStats.totalWork + "\t");
		benchOut.print(PreStats.filterTime + "\t");
		benchOut.print(PreStats.indexTime + "\t");
		benchOut.print(Arrays.toString(PreStats.subPreMillis.toArray()) + "\t");
		benchOut.print(Arrays.toString(JoinStats.subExeTime.toArray()) + "\t");
		benchOut.print(Arrays.toString(JoinStats.subMateriazed.toArray()) + "\t");
		benchOut.println(Arrays.toString(PostStats.subPostMillis.toArray()));
		benchOut.flush();
	}
}
