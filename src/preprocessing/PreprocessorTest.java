package preprocessing;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.TableInfo;
import config.NamingConfig;
import expressions.ExpressionInfo;
import expressions.normalization.CollationVisitor;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import query.ColumnRef;
import query.QueryInfo;

class PreprocessorTest {

	@Test
	void test() throws Exception {
		// Initialize database
		CatalogManager.loadDB("/Users/immanueltrummer/"
				+ "Documents/Temp/SkinnerSchema/imdb");
		// Read all queries from files
		Map<String, PlainSelect> nameToQuery = 
				new TreeMap<String, PlainSelect>();
		File dir = new File("/Users/immanueltrummer/Development"
				+ "/mcts_db_tests/imdb/imdb_queries/remaining");
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
		// Open connection to Postgres 
		String url = "jdbc:postgresql:imdb";
		Properties props = new Properties();
		props.setProperty("user","immanueltrummer");
		props.setProperty("password","");
		Connection connection = DriverManager.getConnection(url, props);
		java.sql.Statement pgStatement = connection.createStatement();
		// Open benchmark result file
		PrintWriter benchOut = new PrintWriter("benchPre.txt");
		// Measure preprocessing time for each query
		benchOut.println("Query\tMillis");
		for (Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
			System.out.println(entry.getKey());
			System.out.println(entry.getValue().toString());
			long startMillis = System.currentTimeMillis();
			QueryInfo query = new QueryInfo(entry.getValue(),
					false, -1, -1, null);
			Preprocessor.process(query);
			long totalMillis = System.currentTimeMillis() - startMillis;
			benchOut.print(entry.getKey() + "\t");
			benchOut.println(totalMillis);
			benchOut.flush();
			// Check consistency with Postgres results
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
				String sql = sqlBuilder.toString();
				System.out.println(sql);
				ResultSet result = pgStatement.executeQuery(sql);
				result.next();
				int pgCardinality = result.getInt(1);
				System.out.println("PG cardinality:\t" + pgCardinality);
				// Get cardinality after Skinner filtering
				String skinnerTable = NamingConfig.FILTERED_PRE + alias;
				TableInfo skinnerInfo = CatalogManager.currentDB.nameToTable.get(skinnerTable);
				String firstColumn = skinnerInfo.columnNames.get(0);
				ColumnRef colRef = new ColumnRef(skinnerTable, firstColumn);
				int skinnerCardinality = BufferManager.colToData.get(colRef).getCardinality();
				System.out.println("Skinner card:\t" + skinnerCardinality);
				assertEquals(pgCardinality, skinnerCardinality);
			}
			BufferManager.unloadTempData();
			CatalogManager.removeTempTables();
		}
		connection.close();
		benchOut.close();
	}
}
