package print;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import data.LongData;
import data.StringData;
import query.ColumnRef;
import types.SQLtype;

/**
 * Contains methods to print out relations.
 * 
 * @author immanueltrummer
 *
 */
public class RelationPrinter {
	/**
	 * Auxiliary function to print separator lines.
	 * 
	 * @param separator	character to print
	 * @param nrRepeats	print character that many times
	 * @param writer	use to print output
	 */
	static void printSeparator(char separator, 
			int nrRepeats, PrintWriter writer) {
		for (int i=0; i<nrRepeats; ++i) {
			writer.print(separator);
		}
		writer.println();
	}
	/**
	 * Print out relation of given name on console.
	 * 
	 * @param tableName	name of table to print
	 */
	public static void print(String tableName, PrintWriter writer) throws Exception {
		// Get table meta-data
		TableInfo tableInfo = CatalogManager.
				currentDB.nameToTable.get(tableName);
		int nrCols = tableInfo.columnNames.size();
		// Print table header
		String header = StringUtils.join(tableInfo.columnNames, "\t");
		int headerLength = header.length() + nrCols * 7;
		printSeparator('-', headerLength, writer);
		writer.println(header);
		printSeparator('-', headerLength, writer);
		// Extract column types
		List<SQLtype> colTypes = new ArrayList<SQLtype>();
		for (String colName : tableInfo.columnNames) {
			ColumnRef colRef = new ColumnRef(tableName, colName);
			ColumnInfo colInfo = CatalogManager.getColumn(colRef);
			colTypes.add(colInfo.type);
		}
		// Get table data
		List<ColumnData> colsData = new ArrayList<ColumnData>();
		for (String colName : tableInfo.columnNames) {
			ColumnRef colRef = new ColumnRef(tableName, colName);
			ColumnData data = BufferManager.getData(colRef);
			colsData.add(data);
		}
		int cardinality = colsData.isEmpty()?0:colsData.get(0).getCardinality();
		// Print out table content
		for (int rowCtr=0; rowCtr<cardinality; ++rowCtr) {
			for (int colCtr=0; colCtr<nrCols; ++colCtr) {
				ColumnData colData = colsData.get(colCtr);
				SQLtype type = colTypes.get(colCtr);
				writer.print(printCell(type, colData, rowCtr));
				writer.print("\t");
			}
			writer.println();
		}
		printSeparator('-', headerLength, writer);
		writer.flush();
	}
	/**
	 * Print out cell content, formatted according to given data type.
	 * 
	 * @param type	data type of column
	 * @param data	column content
	 * @param rowNr	print cell in this row
	 */
	static String printCell(SQLtype type, ColumnData data, int rowNr) {
		// Check for null values
		if (data.isNull.get(rowNr)) {
			return "[null]";
		} else {
			switch (type) {
			case INT:
				return Integer.valueOf(((IntData)data).data[rowNr]).toString();
			case LONG:
				return Long.valueOf(((LongData)data).data[rowNr]).toString();
			case DOUBLE:
				return Double.valueOf(((DoubleData)data).data[rowNr]).toString();
			case STRING_CODE:
				int code = Integer.valueOf(((IntData)data).data[rowNr]);
				return BufferManager.dictionary.getString(code);
			case STRING:
				return ((StringData)data).data[rowNr];
			default:
				return "Error - Unsupported output type!";
			}			
		}
	}
}
