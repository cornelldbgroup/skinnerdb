package operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.TableInfo;
import data.ColumnData;
import query.ColumnRef;

/**
 * Performs an in-place sort of the rows in a relation,
 * using a given list of columns to specify sort order.
 * 
 * @author immanueltrummer
 *
 */
public class OrderBy {
	/**
	 * Sorts the rows of the given relation, according to
	 * a prioritized list of sort columns with associated
	 * sort directions.
	 * 
	 * @param orderCols		references to columns specifying sort order
	 * @param orderIsAsc	whether to use ascending for specific column
	 * @param relToSort		the relation whose rows to sort
	 * @throws Exception
	 */
	public static void execute(List<ColumnRef> orderCols, 
			boolean[] orderIsAsc, String relToSort) throws Exception {
		// Resolve column and relation references
		List<ColumnData> orderData = new ArrayList<>();
		for (ColumnRef orderRef : orderCols) {
			ColumnData curData = BufferManager.getData(orderRef);
			orderData.add(curData);
		}
		List<ColumnData> dataToSort = new ArrayList<>();
		TableInfo resultInfo = CatalogManager.currentDB.nameToTable.get(relToSort);
		for (String colName : resultInfo.columnNames) {
			ColumnRef colRef = new ColumnRef(relToSort, colName);
			ColumnData colData = BufferManager.getData(colRef);
			// Make sure that we do not swap rows in the same
			// column multiple twice.
			if (!orderData.contains(colData)) {
				dataToSort.add(colData);				
			}
		}
		// Sort rows
		int cardinality = CatalogManager.getCardinality(relToSort);
		quicksort(orderData, orderIsAsc, dataToSort, 0, cardinality-1);
	}
	/**
	 * Sort rows in the given range of row indices, using a list
	 * of columns to sort by with associated sort direction.
	 * 
	 * @param orderData		this data specifies sort order
	 * @param orderIsAsc	specifies sort direction for each column
	 * @param toSort		data to sort
	 * @param lb			lower bound of sort range
	 * @param ub			upper bound of sort range
	 */
	static void quicksort(List<ColumnData> orderData, boolean[] orderIsAsc,
			Collection<ColumnData> toSort, int lb, int ub) {
		// Anything left to sort?
		if (lb<ub) {
			// Divide range to sort according to pivot element
			int p = partition(orderData, orderIsAsc, toSort, lb, ub);
			// Recursively sort two resulting partitions
			quicksort(orderData, orderIsAsc, toSort, lb, p-1);
			quicksort(orderData, orderIsAsc, toSort, p+1, ub);
		}
	}
	/**
	 * Partitioning step of Quicksort: select pivot element and
	 * swap elements depending on comparison against pivot element.
	 * 
	 * @param orderData		use this data to determine sort order
	 * @param orderIsAsc	specifies sort direction for each sort column
	 * @param toSort		the data to sort
	 * @param lb			lower bound of row index range to sort
	 * @param ub			upper bound of row index range to sort
	 * @return 	row index starting from which larger elements than pivot are found
	 */
	static int partition(List<ColumnData> orderData, boolean[] orderIsAsc, 
			Collection<ColumnData> toSort, int lb, int ub) {
		int pivotRow = ub;
		int i = lb;
		for (int j=lb; j<ub; ++j) {
			if (compare(orderData, orderIsAsc, j, pivotRow)<0) {
				// Swap rows accordingly
				swap(orderData, toSort, i, j);
				++i;
			}
		}
		swap(orderData, toSort, i, ub);
		return i;
	}
	/**
	 * Swap two rows in all columns.
	 * 
	 * @param orderData		data used for sorting
	 * @param toSort		data to sort
	 * @param row1			index of first swapped row
	 * @param row2			index of second swapped row
	 */
	static void swap(List<ColumnData> orderData, 
			Collection<ColumnData> toSort, 
			int row1, int row2) {
		for (ColumnData colData : orderData) {
			colData.swapRows(row1, row2);
		}
		for (ColumnData colData : toSort) {
			colData.swapRows(row1, row2);
		}
	}
	/**
	 * Compares two rows according to all sort columns. Returns
	 * -1 if first row comes first according to specified order,
	 * 1 if second row is ordered after first one, and zero
	 * if both rows are equivalent.
	 * 
	 * @param orderData		data of sort columns
	 * @param orderIsAsc	sort direction for each sort column
	 * @param row1			index of first compared row
	 * @param row2			index of second compared row
	 * @return				-1 if row1<row2, 1 if row1>row2, 0 otherwise
	 */
	static int compare(List<ColumnData> orderData, 
			boolean[] orderIsAsc, int row1, int row2) {
		int nrOrderCols = orderData.size();
		for (int orderCtr=0; orderCtr<nrOrderCols; ++orderCtr) {
			ColumnData curData = orderData.get(orderCtr);
			boolean curAsc = orderIsAsc[orderCtr];
			// Comparison result based on is-null flags
			boolean isNullRow1 = curData.isNull.get(row1);
			boolean isNullRow2 = curData.isNull.get(row2);
			if (isNullRow1 && !isNullRow2) {
				return curAsc?1:-1;
			} else if (!isNullRow1 && isNullRow2) {
				return curAsc?-1:1;
			} else {
				// Comparison result based on data content
				int curCmp = curData.compareRows(row1, row2);
				if (curCmp != 0) {
					return curAsc?curCmp:-curCmp;
				}
			}
		}
		return 0;
	}
}
