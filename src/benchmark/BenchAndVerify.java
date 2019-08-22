package benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

import config.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Map.Entry;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.TableInfo;
import diskio.PathUtil;
import expressions.ExpressionInfo;
import expressions.normalization.CollationVisitor;
import expressions.printing.PgPrinter;
import indexing.Indexer;
import joining.JoinProcessor;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import print.RelationPrinter;
import query.ColumnRef;
import query.QueryInfo;
import statistics.BufferStats;
import statistics.JoinStats;

/**
 * Benchmarks pre- and join-processing stage and compares
 * the output sizes against the sizes of results produced
 * by Postgres.
 * 
 * @author immanueltrummer
 *
 */
public class BenchAndVerify {
	/**
	 * Processes all queries in given directory.
	 * 
	 * @param args	first argument is DB directory, 
	 * 				second argument is query directory
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Check for command line parameters
		if (args.length != 2) {
			System.out.println("Specify DB dir and query directory!");
			return;
		}
		// Initialize database
		String dbDir = args[0];
		PathUtil.initSchemaPaths(dbDir);
		CatalogManager.loadDB(PathUtil.schemaPath);
		PathUtil.initDataPaths(CatalogManager.currentDB);
		System.out.println("Loading data ...");
		GeneralConfig.inMemory = true;
		// Load data and/or dictionary
		// In-memory data processing
		BufferManager.loadDB();

		System.out.println("Data loaded.");
		Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
		// Read all queries from files
		Map<String, PlainSelect> nameToQuery = 
				BenchUtil.readAllQueries(args[1]);
		// Open connection to Postgres 
		String url = "jdbc:postgresql:imdb_unicode_index";
		Properties props = new Properties();
		props.setProperty("user","postgres");
		props.setProperty("password","");
		Connection connection = DriverManager.getConnection(url, props);
		java.sql.Statement pgStatement = connection.createStatement();
		// Open benchmark result file
		PrintWriter benchOut = new PrintWriter("bench.txt");
		PrintStream pgOut = new PrintStream("pgResults.txt");
		PrintStream skinnerOut = new PrintStream("skinnerResults.txt");
		PrintStream console = System.out;
		// Measure preprocessing time for each query
		BenchUtil.writeBenchHeader(benchOut);
		int count = 0;
		for (Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
			if (count > 0) {
				break;
			}
			count++;
			System.out.println(entry.getKey());
			System.out.println(entry.getValue().toString());
			long startMillis = System.currentTimeMillis();
			QueryInfo query = new QueryInfo(entry.getValue(),
					false, -1, -1, null);
			Context preSummary = Preprocessor.process(query);
			long joinStart = System.currentTimeMillis();
			long preMillis = joinStart - startMillis;
			JoinProcessor.process(query, preSummary);
			long postStartMillis = System.currentTimeMillis();
			long joinMillis = postStartMillis - joinStart;
			PostProcessor.process(query, preSummary);
			long postMillis = System.currentTimeMillis() - postStartMillis;
			long totalMillis = System.currentTimeMillis() - startMillis;
			// Check consistency with Postgres results: unary preds
			if (TestConfig.CHECK_CORRECTNESS) {
				for (ExpressionInfo expr : query.unaryPredicates) {
					// Unary predicates must refer to one table
					assertEquals(expr.aliasesMentioned.size(), 1);
					// Get cardinality after PG filtering
					String alias = expr.aliasesMentioned.iterator().next();
					String table = query.aliasToTable.get(alias);
					StringBuilder sqlBuilder = new StringBuilder();
					sqlBuilder.append("SELECT COUNT(*) FROM ");
					sqlBuilder.append(table);
					sqlBuilder.append(" AS ");
					sqlBuilder.append(alias);
					sqlBuilder.append(" WHERE ");
					CollationVisitor collator = new CollationVisitor();
					expr.originalExpression.accept(collator);
					sqlBuilder.append(collator.exprStack.pop().toString());
					String sql = sqlBuilder.toString().replace("STRING", "TEXT");
					System.out.println(sql);
					ResultSet result = pgStatement.executeQuery(sql);
					result.next();
					int pgCardinality = result.getInt(1);
					System.out.println("PG cardinality:\t" + pgCardinality);
					// Get cardinality after Skinner filtering
					String filteredName = preSummary.aliasToFiltered.get(alias);
					TableInfo filteredTable = CatalogManager.currentDB.
							nameToTable.get(filteredName);
					String columnName = filteredTable.nameToCol.keySet().iterator().next();
					ColumnRef colRef = new ColumnRef(filteredName, columnName);
					int skinnerCardinality = BufferManager.colToData.get(colRef).getCardinality();
					System.out.println("Skinner card:\t" + skinnerCardinality);
					assertEquals(pgCardinality, skinnerCardinality);
				}

				// Check consistency with Postgres: join result size
				StringBuilder sqlBuilder = new StringBuilder();
				sqlBuilder.append("SELECT COUNT(*) FROM ");
				List<String> fromItems = query.aliasToTable.entrySet().stream().
						map(e -> e.getValue() + " " + e.getKey()).
						collect(Collectors.toList());
				String fromClause = StringUtils.join(fromItems, ", ");
				sqlBuilder.append(fromClause);
				if (!query.wherePredicates.isEmpty()) {
					sqlBuilder.append(" WHERE ");
					String whereCondition = StringUtils.join(
							query.wherePredicates, " AND ");
					sqlBuilder.append(whereCondition);
				}
				String joinSql = sqlBuilder.toString().replace("STRING", "TEXT");
				System.out.println("Join query: " + joinSql);
				ResultSet joinResult = pgStatement.executeQuery(joinSql);
				joinResult.next();
				int pgJoinCard = joinResult.getInt(1);

				// Get cardinality of Skinner join result
				int skinnerJoinCard = CatalogManager.getCardinality(
						NamingConfig.JOINED_NAME);
				System.out.println("PG Card: " + pgJoinCard +
						"; Skinner card: " + skinnerJoinCard);
				assertEquals(pgJoinCard, skinnerJoinCard);
				// Output final result for Postgres
				StringBuilder pgBuilder = new StringBuilder();
				PlainSelect plainSelect = entry.getValue();
				pgBuilder.append("SELECT ");
				boolean firstSelectItem = true;
				for (ExpressionInfo selExpr : query.selectExpressions) {
					if (firstSelectItem) {
						firstSelectItem = false;
					} else {
						pgBuilder.append(", ");
					}
					PgPrinter pgPrinter = new PgPrinter(query);
					pgPrinter.setBuffer(pgBuilder);
					selExpr.afterNormalization.accept(pgPrinter);
				}
				pgBuilder.append(" FROM ");
				pgBuilder.append(fromClause);
				pgBuilder.append(" WHERE ");
				CollationVisitor collator = new CollationVisitor();
				plainSelect.getWhere().accept(collator);
				pgBuilder.append(collator.exprStack.pop().toString());
				String pgQuery = pgBuilder.toString().replace("STRING", "TEXT");
				System.out.println("PG Query: " + pgQuery);
				ResultSet queryResult = pgStatement.executeQuery(pgQuery);
				int nrPgCols = queryResult.getMetaData().getColumnCount();
				while (queryResult.next()) {
					for (int colCtr=1; colCtr<=nrPgCols; ++colCtr) {
						pgOut.print(queryResult.getString(colCtr) + "\t");
					}
					pgOut.println();
				}
				pgOut.flush();
			}

			int skinnerJoinCard = CatalogManager.getCardinality(
					NamingConfig.JOINED_NAME);
			// Output final result for Skinner
			String resultRel = NamingConfig.FINAL_RESULT_NAME;
			System.setOut(skinnerOut);
			RelationPrinter.print(resultRel);
			skinnerOut.flush();
			System.setOut(console);
			// Generate output
			benchOut.print(entry.getKey() + "\t");
			benchOut.print(totalMillis + "\t");
			benchOut.print(preMillis + "\t");
			benchOut.print(joinMillis + "\t");
			benchOut.print(postMillis + "\t");
			benchOut.print(JoinStats.nrTuples + "\t");
			benchOut.print(JoinStats.nrIterations + "\t");
			benchOut.print(JoinStats.nrIndexLookups + "\t");
			benchOut.print(JoinStats.nrIndexEntries + "\t");
			benchOut.print(JoinStats.nrUniqueIndexLookups + "\t");
			benchOut.print(JoinStats.nrUctNodes + "\t");
			benchOut.print(JoinStats.nrPlansTried + "\t");
			benchOut.print(skinnerJoinCard + "\t");
			benchOut.print(JoinStats.nrSamples + "\t");
			benchOut.print(JoinStats.avgReward + "\t");
			benchOut.print(JoinStats.maxReward + "\t");
			benchOut.print(JoinStats.totalWork + "\t");
			benchOut.print(BufferStats.nrIndexLookups + "\t");
			benchOut.print(BufferStats.nrCacheHit + "\t");
			benchOut.print(BufferStats.nrCacheMiss + "\t");
			double hitRatio = BufferStats.nrIndexLookups == 0 ? 0 : (BufferStats.nrCacheHit + 0.0) / BufferStats.nrIndexLookups;
			benchOut.println(hitRatio);
			benchOut.flush();
			// Clean up
			BufferManager.unloadTempData();
			CatalogManager.removeTempTables();
		}
		connection.close();
		benchOut.close();
		pgOut.close();
		skinnerOut.close();
	}

}
