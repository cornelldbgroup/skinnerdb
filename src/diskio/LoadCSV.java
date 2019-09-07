package diskio;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVReader;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import catalog.stats.TableStats;
import config.GeneralConfig;
import data.ColumnData;
import data.DoubleData;
import data.IntData;
import data.LongData;
import data.StringData;
import query.ColumnRef;
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

/**
 * Loads CSV file into table.
 * 
 * @author immanueltrummer
 *
 */
public class LoadCSV {
	/**
	 * Counts the number of lines in specified text file.
	 * 
	 * @param path	path to text file
	 * @return		number of lines in file
	 * @throws Exception
	 */
	static int lineCount(String path) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		@SuppressWarnings("unused")
		String line = null;
		int lineCtr = 0;
		while ((line = reader.readLine()) != null) {
			++lineCtr;
		}
		reader.close();
		return lineCtr;
	}
	/**
	 * Initializes objects holding column data for given table.
	 * 
	 * @param table			table (defining column types)
	 * @param cardinality	number of rows to load
	 * @return				list of column data objects
	 * @throws Exception
	 */
	static List<ColumnData> initData(TableInfo table, 
			int cardinality) throws Exception {
		List<ColumnData> data = new ArrayList<>();
		for (String columnName : table.columnNames) {
			ColumnInfo column = table.nameToCol.get(columnName);
			JavaType javaType = TypeUtil.toJavaType(column.type); 
			switch (javaType) {
			case INT:
				data.add(new IntData(cardinality));
				break;
			case LONG:
				data.add(new LongData(cardinality));
				break;
			case DOUBLE:
				data.add(new DoubleData(cardinality));
				break;
			case STRING:
				data.add(new StringData(cardinality));
				break;
			default:
				throw new Exception("Unsupported column type");
			}
		}
		return data;
	}
	/**
	 * Parse data for table from CSV file.
	 * 
	 * @param csvPath				path to source CSV file
	 * @param table					table for which to parse data
	 * @param data					store parsed data here
	 * @param separator				sign separating fields in .csv file
	 * @param nullRepresentation	representation of null values
	 */
	static void parseData(String csvPath, TableInfo table, 
			List<ColumnData> data, char separator, 
			String nullRepresentation) throws Exception {
		// Extract column types for quick access
		int nrColumns = table.columnNames.size();
		SQLtype[] columnTypes = new SQLtype[nrColumns];
		for (int colCtr=0; colCtr<nrColumns; ++colCtr) {
			String colName = table.columnNames.get(colCtr);
			ColumnInfo column = table.nameToCol.get(colName);
			columnTypes[colCtr] = column.type;
		}
		// Open CSV file for reading
		CSVReader csvReader = new CSVReader(
				new FileReader(csvPath), separator);
		String[] inputFields;
		int rowCtr = 0;
		while ((inputFields = csvReader.readNext()) != null) {
			for (int colCtr=0; colCtr<nrColumns; ++colCtr) {
				String field = inputFields[colCtr];
				boolean isNull = field==null||
						field.isEmpty()||
						field.equals(nullRepresentation);
				data.get(colCtr).isNull.set(rowCtr, isNull);
				try {
					switch (columnTypes[colCtr]) {
					case ANY_TYPE:
						throw new Exception("Cannot parse undetermined type");
					case BYTE:
					case INT:
						IntData intData = ((IntData)data.get(colCtr));
						intData.data[rowCtr] = isNull?0:Integer.parseInt(field);
						break;
					case LONG:
						LongData longData = ((LongData)data.get(colCtr));
						longData.data[rowCtr] = isNull?0:Long.parseLong(field);
						break;
					case DOUBLE:
						DoubleData doubleData = ((DoubleData)data.get(colCtr));
						doubleData.data[rowCtr] = isNull?0:Double.parseDouble(field);
						break;
					case STRING:
						StringData stringData = ((StringData)data.get(colCtr));
						stringData.data[rowCtr] = isNull?nullRepresentation:field; 
						break;
					case DATE:
						IntData dateData = ((IntData)data.get(colCtr));
						if (!isNull) {
							Date date = Date.valueOf(field);
							dateData.data[rowCtr] = (int)(
									date.getTime()/(long)1000);							
						}
						break;
					case TIME:
						IntData timeData = ((IntData)data.get(colCtr));
						if (!isNull) {
							Time time = Time.valueOf(field);
							timeData.data[rowCtr] = (int)(
									time.getTime()/(long)1000);
						}
						break;
					case TIMESTAMP:
						IntData tsData = ((IntData)data.get(colCtr));
						if (!isNull) {
							Timestamp ts = Timestamp.valueOf(field);
							tsData.data[rowCtr] = (int)(
									ts.getTime()/(long)1000);
						}
						break;
					default:
						throw new Exception("Unsupported type: " + 
								columnTypes[colCtr]);
					}					
				} catch (Exception e) {
					System.err.println("Error parsing field " + field + 
							" in column " + colCtr + " of line " + rowCtr);
					throw e;
				}
			}
			++rowCtr;
			if (rowCtr % 100000 == 0) {
				System.out.println("Loaded " + rowCtr + " rows");
			}
		}
		csvReader.close();
	}
	/**
	 * Stores column data on disk and in buffer pool if
	 * in-memory processing is activated.
	 * 
	 * @param table		table for which to store data
	 * @param data		data to store
	 * @throws Exception
	 */
	static void storeData(TableInfo table, List<ColumnData> data) throws Exception {
		int nrColumns = table.columnNames.size();
		for (int colCtr=0; colCtr<nrColumns; ++colCtr) {
			// Store data on hard disk
			String columnName = table.columnNames.get(colCtr);
			ColumnInfo column = table.nameToCol.get(columnName);
			ColumnData colData = data.get(colCtr);
			String dataPath = PathUtil.colToPath.get(column);
			colData.store(dataPath);
			// Load data into buffer pool if required
			if (GeneralConfig.inMemory) {
				String tableName = table.name;
				ColumnRef colRef = new ColumnRef(tableName, columnName);
				BufferManager.colToData.put(colRef, colData);
			}
		}
	}
	/**
	 * Overrides table content on hard disk for given table
	 * with the contents extracted from CSV file.
	 * 
	 * @param csvPath				path to source CSV file
	 * @param table					table whose content to override
	 * @param separator				character separating CSV fields
	 * @param nullRepresentation	how the null value is represented
	 * @throws Exception
	 */
	public static void load(String csvPath, TableInfo table, 
			char separator, String nullRepresentation) throws Exception {
		System.out.println("Loading data for table " + table);
		// Determine number of lines in CSV file
		int cardinality = lineCount(csvPath);
		System.out.println("Loading " + cardinality + " rows ...");
		// Create objects for holding data
		List<ColumnData> data = initData(table, cardinality);
		// Parse data from CSV file
		parseData(csvPath, table, data, separator, nullRepresentation);
		// Store column data to hard disk
		storeData(table, data);
		System.out.println("Stored table on disk");
		// Update cardinality estimates
		String tableName = table.name;
		CatalogManager.updateStats(tableName);
		TableStats tableStats = CatalogManager.currentStats.tableToStats.get(tableName);
		System.out.println("Updated table statistics: ");
		System.out.println(tableStats);
		// Load data into buffer for main memory processing
		if (GeneralConfig.inMemory) {
			for (String col : table.columnNames) {
				ColumnRef colRef = new ColumnRef(tableName, col);
				BufferManager.loadColumn(colRef);
			}
		}
	}
}
