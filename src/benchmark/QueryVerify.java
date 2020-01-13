package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.NamingConfig;
import config.StartupConfig;
import diskio.PathUtil;
import expressions.ExpressionInfo;
import expressions.normalization.CollationVisitor;
import expressions.printing.PgPrinter;
import indexing.Indexer;
import joining.JoinProcessor;
import joining.ParallelJoinProcessor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import postprocessing.ParallelPostProcessor;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import print.RelationPrinter;
import query.QueryInfo;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Compares the output sizes against the sizes of results produced by
 * Postgres or MonetDB.
 *
 * @author Ziyun Wei
 *
 */
public class QueryVerify {
    /**
     * Processes all queries in given directory.
     *
     * @param args	first argument is Skinner DB directory,
     * 				second argument is query directory
     * 				third argument is Postgres database name
     * 				fourth argument is Postgres user name
     * 				fifth argument is Postgres user password
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Check for command line parameters
        if (args.length != 4 && args.length != 5) {
            System.out.println("Specify Skinner DB dir, "
                    + "query directory, Postgres DB name, "
                    + "Postgres user, and Postgres password!");
            return;
        }
        // Initialize database
        String SkinnerDbDir = args[0];
        String queryDir = args[1];
        String PgDB = args[2];
        String PgUser = args[3];
        String PgPassword = args.length==5?args[4]:"";
        PathUtil.initSchemaPaths(SkinnerDbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        System.out.println("Loading data ...");
        GeneralConfig.inMemory = true;
		BufferManager.loadDB();
        System.out.println("Data loaded.");
		Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
        // Read all queries from files
        Map<String, Statement> nameToQuery =
                BenchUtil.readAllQueries(queryDir);
        // Open connection to Postgres
        String url = "jdbc:postgresql:" + PgDB;
        Properties props = new Properties();
        props.setProperty("user",PgUser);
        props.setProperty("password",PgPassword);
        Connection connection = DriverManager.getConnection(url, props);
        java.sql.Statement pgStatement = connection.createStatement();
        // Open benchmark result file
        PrintWriter benchOut = new PrintWriter("bench.txt");
        PrintStream pgOut = new PrintStream("pgResults.txt");

        for (Map.Entry<String, Statement> entry : nameToQuery.entrySet()) {
            System.out.println(entry.getKey());
            System.out.println(entry.getValue().toString());


			QueryInfo query = new QueryInfo((PlainSelect) entry.getValue(),
					false, -1, -1, null);
			Context preSummary = Preprocessor.process(query);
			ParallelJoinProcessor.process(query, preSummary);
			ParallelPostProcessor.process(query, preSummary,
					NamingConfig.FINAL_RESULT_NAME, true);


            // Output final result for Postgres
            StringBuilder pgBuilder = new StringBuilder();
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
//			pgBuilder.append(fromClause);
			pgBuilder.append(" WHERE ");
			CollationVisitor collator = new CollationVisitor();
//			plainSelect.getWhere().accept(collator);
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
            // Output final result for Skinner
            String resultRel = NamingConfig.FINAL_RESULT_NAME;
//            System.setOut(skinnerOut);
            RelationPrinter.print(resultRel);
//            skinnerOut.flush();
//            System.setOut(console);
            // Clean up
            BufferManager.unloadTempData();
            CatalogManager.removeTempTables();
        }
        connection.close();
    }
}
