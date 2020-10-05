package benchmark;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import catalog.CatalogManager;
import com.sun.jna.platform.win32.FlagEnum;
import config.GeneralConfig;
import config.NamingConfig;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

/**
 * Several auxiliary methods for benchmarking SkinnerMT.
 * 
 * @author Anonymous
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
				List<Statement> statements = new ArrayList<>();
				while (scanner.hasNext()) {
					String sqlCmd = scanner.next().trim();
					if (!sqlCmd.equals("")) {
						System.out.println(sqlCmd);
						try {
							Statement sqlStatement = CCJSqlParserUtil.parse(sqlCmd);
							statements.add(0, sqlStatement);
						} catch (Exception e) {
							throw new Exception("query parse error!");
						}
					}
				}
				if (statements.size() == 1) {
					String queryName = file.getName();
					nameToQuery.put(queryName, statements.get(0));
				}
				else {
					for (int i = 0; i < statements.size(); i++) {
						String queryName = file.getName().split(".sql")[0] + (char)('a' + i) + ".sql";
						nameToQuery.put(queryName, statements.get(i));
					}
				}
			}
			scanner.close();
		}
		return nameToQuery;
	}

	public static Map<String, int[]> readOptimalJoinOrders(String path) throws IOException {
		if (path == null) {
			return new HashMap<>();
		}
		File optimalFile = new File(path);
		BufferedReader br = new BufferedReader(new FileReader(optimalFile));
		Map<String, int[]> optimalOrders = new HashMap<>();
		String st;
		while ((st = br.readLine()) != null) {
			String[] information = st.split("\t");
			String query = information[0];
			String order = information[1];
			String[] tables = order.substring(1, order.length() - 1).split(", ");
			int[] optimal = new int[tables.length];
			for (int i = 0; i < tables.length; i++) {
				optimal[i] = Integer.parseInt(tables[i]);
			}
			optimalOrders.put(query, optimal);
		}
		return optimalOrders;
	}

	/**
	 * Writes header row of benchmark result file.
	 * 
	 * @param benchOut	channel to benchmark file
	 */
	public static void writeBenchHeader(PrintWriter benchOut) {
		benchOut.println("Query\tIsWarmup\tMillis\tPreMillis\tJoinMillis\tMatMillis\tPostMillis\t"
				+ "FilterMillis\tIndexMillis\tGroupByMillis\tAggregateMillis\tHavingMillis\tOrderMillis\t"
				+ "Tuples\tSamples\tLookups\tNrIndexEntries\tnrUniqueLookups\t"
				+ "NrPlans\tJoinCard\tAvgReward\tMaxReward\tTotalWork\t"
				+ "DataSize\tUctSize\tStateSize\tJoinSize");
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
			PrintWriter benchOut) {
		// Generate output
		benchOut.print(queryName + "\t");
		benchOut.print(!GeneralConfig.ISTESTCASE + "\t");
		benchOut.print(totalMillis + "\t");
		benchOut.print(PreStats.preMillis + "\t");
		benchOut.print(JoinStats.joinMillis + "\t");
		benchOut.print(JoinStats.matMillis + "\t");
		benchOut.print(PostStats.postMillis + "\t");
		// Break down statistics
		benchOut.print(PreStats.filterMillis + "\t");
		benchOut.print(PreStats.indexMillis + "\t");
		benchOut.print(PostStats.groupByMillis + "\t");
		benchOut.print(PostStats.aggMillis + "\t");
		benchOut.print(PostStats.havingMillis + "\t");
		benchOut.print(PostStats.orderMillis + "\t");
		// Additional statistics
		benchOut.print(JoinStats.nrTuples + "\t");
		benchOut.print(JoinStats.nrSamples + "\t");
		benchOut.print(JoinStats.nrIndexLookups + "\t");
		benchOut.print(JoinStats.nrIndexEntries + "\t");
		benchOut.print(JoinStats.nrUniqueIndexLookups + "\t");
		benchOut.print(JoinStats.nrPlansTried + "\t");
		benchOut.print(JoinStats.lastJoinCard + "\t");
		benchOut.print(JoinStats.avgReward + "\t");
		benchOut.print(JoinStats.maxReward + "\t");
		benchOut.print(JoinStats.totalWork + "\t");
		// Memory consumption statistics
		benchOut.print(JoinStats.dataSize + "\t");
		benchOut.print(JoinStats.treeSize + "\t");
		benchOut.print(JoinStats.stateSize + "\t");
		benchOut.println(JoinStats.joinSize);
		benchOut.flush();
	}
}
