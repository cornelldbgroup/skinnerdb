package benchmark;

import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Import the imdb dataset to MonetDB
 *
 * There are some bugs of the MonetDB parser.
 * If we use COPY command like, COPY INTO link_type FROM '/home/skinner/imdb/link_type.csv' USING DELIMITERS ',', '\n' ,'"' NULL as '';
 * 3 tables are failed to import.
 *
 * Thus, we use the Java OpenCSV library to parse the data and construct insert commands.
 *
 * @author Junxiong Wang
 */
public class ImportImdbMonetdb {

    Statement statement;
    public final static String username = "monetdb";
    public final static String password = "monetdb";

    public ImportImdbMonetdb(String dbName) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:monetdb://localhost:50000/" + dbName, username, password);
        statement = conn.createStatement();
    }

    private void importTable(String dataFolder, String tableName) throws IOException {
        CSVReader reader = new CSVReader(new FileReader(String.format("%s%s.csv", dataFolder, tableName)));
        String[] attributes;
        while ((attributes = reader.readNext()) != null) {
            String[] insertTuple = new String[attributes.length];
            for (int i = 0; i < attributes.length; i++) {
                String attribute = attributes[i];
                insertTuple[i] = (attribute.trim().length() > 0) ? "'" + attribute + "'" : null;
                if(insertTuple[i] != null) {
                    insertTuple[i] = insertTuple[i].replace("'","\'");
                    insertTuple[i] = insertTuple[i].replace("\"","\\\"");
                }
            }
            String insert = String.format("insert into %s values(%s)", tableName, String.join(",", insertTuple));
            System.out.println(insert);
            try {
                statement.execute(insert);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void importDatabase(String dataFolder) throws Exception {
        String[] tableNames = {"person_info","title","complete_cast"};
        /*
        String[] tableNames = {"aka_name", "aka_title", "cast_info", "char_name",
                "comp_cast_type", "company_name", "company_type",
                "complete_cast", "info_type", "keyword", "kind_type",
                "link_type", "movie_companies", "movie_info",
                "movie_info_idx", "movie_keyword", "movie_link",
                "name", "person_info", "role_type", "title"};
        */
        for (String tableName : tableNames)
            importTable(dataFolder, tableName);
    }

    public static void main(String[] argv) throws Exception {
        ImportImdbMonetdb importImdbMonetdb = new ImportImdbMonetdb("imdb");
        importImdbMonetdb.importDatabase(argv[0]);
    }
}