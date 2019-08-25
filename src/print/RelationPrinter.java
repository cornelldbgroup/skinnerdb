package print;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

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
	 */
	static void printSeparator(char separator, int nrRepeats) {
		for (int i=0; i<nrRepeats; ++i) {
			System.out.print(separator);
		}
		System.out.println();
	}
	/**
	 * Print out relation of given name on console.
	 * 
	 * @param tableName	name of table to print
	 */
	public static void print(String tableName) throws Exception {
		// Get table meta-data
		TableInfo tableInfo = CatalogManager.
				currentDB.nameToTable.get(tableName);
		int nrCols = tableInfo.columnNames.size();
		// Print table header
		String header = StringUtils.join(tableInfo.columnNames, "\t");
		int headerLength = header.length() + nrCols * 7;
		printSeparator('-', headerLength);
		System.out.println(header);
		printSeparator('-', headerLength);
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
		int cardinality = CatalogManager.getCardinality(tableName);
		// Print out table content
		for (int rowCtr=0; rowCtr<cardinality; ++rowCtr) {
			for (int colCtr=0; colCtr<nrCols; ++colCtr) {
				ColumnData colData = colsData.get(colCtr);
				SQLtype type = colTypes.get(colCtr);
				System.out.print(printCell(type, colData, rowCtr));
				System.out.print("\t");
			}
			System.out.println();
		}
		printSeparator('-', headerLength);
		System.out.flush();
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
				int code = ((IntData)data).data[rowNr];
				return BufferManager.dictionary.getString(code);
			case STRING:
				return ((StringData)data).data[rowNr];
			case DATE:
			case TIME:
			case TIMESTAMP:
				int unixTime = ((IntData)data).data[rowNr];
				long millisSince1970 = unixTime * 1000l;
				// Print out datetime in appropriate format
				if (type.equals(SQLtype.TIME)) {
					Time time = new Time(millisSince1970);
					return time.toString();
				} else if (type.equals(SQLtype.DATE)) {
					Date date = new Date(millisSince1970);
					return date.toString();
				} else {
					Timestamp timestamp = new Timestamp(millisSince1970);
					return timestamp.toString();
				}
			case YM_INTERVAL:
				int totalMonths = ((IntData)data).data[rowNr];
				int years = totalMonths / 12;
				int remainingMonths = totalMonths % 12;
				return years + " year" + (years!=1?"s":"") + " " +
						remainingMonths + " month" + 
						(remainingMonths!=1?"s":"");
			case DT_INTERVAL:
				int durationSecs = ((IntData)data).data[rowNr];
				long durationMillis = 1000 * durationSecs;
				return DurationFormatUtils.formatDurationISO(durationMillis);
			default:
				return "Error - Unsupported output type " + type + "!";
			}
		}
	}
}
