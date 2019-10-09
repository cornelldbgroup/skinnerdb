package operators;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import buffer.BufferManager;
import config.ParallelConfig;
import expressions.normalization.PlainVisitor;
import indexing.Index;
import indexing.ThreadIntIndex;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import query.ColumnRef;
import query.QueryInfo;

/**
 * Uses all applicable indices to evaluate a unary
 * predicate and returns set of qualifying row
 * indices. The index filter should only be applied
 * to expressions that pass the IndexTest.
 * 
 * @author immanueltrummer
 *
 */
public class IndexFilter extends PlainVisitor {
	/**
	 * Query to which index filter is applied.
	 */
	final QueryInfo query;
	/**
	 * Contains indexes of all rows satisfying
	 * the predicate.
	 */
	public final Deque<List<Integer>> qualifyingRows = 
			new ArrayDeque<>();
	/**
	 * Contains last extracted integer constants.
	 */
	final Deque<Integer> extractedConstants =
			new ArrayDeque<>();
	/**
	 * Contains indexes applicable
	 * for sub-expressions.
	 */
	final Deque<Index> applicableIndices =
			new ArrayDeque<>();
	/**
	 * Initialize index filter for given query.
	 * 
	 * @param query	meta-data on evaluated query
	 */
	public IndexFilter(QueryInfo query) {
		this.query = query;
	}
	
	@Override
	public void visit(AndExpression and) {
		and.getLeftExpression().accept(this);
		and.getRightExpression().accept(this);
		// Intersect sorted row index lists
		List<Integer> rows1 = qualifyingRows.pop();
		List<Integer> rows2 = qualifyingRows.pop();
		List<Integer> intersectedRows = new ArrayList<Integer>();
		qualifyingRows.push(intersectedRows);
		if (!rows1.isEmpty() && !rows2.isEmpty()) {
			Iterator<Integer> row1iter = rows1.iterator();
			Iterator<Integer> row2iter = rows2.iterator();
			int row1 = row1iter.next();
			int row2 = row2iter.next();
			while (row1 !=Integer.MAX_VALUE && 
					row2 != Integer.MAX_VALUE) {
				if (row1 == row2) {
					intersectedRows.add(row1);
					row1 = row1iter.hasNext()?row1iter.next():Integer.MAX_VALUE;
					row2 = row2iter.hasNext()?row2iter.next():Integer.MAX_VALUE;
				} else if (row1 < row2) {
					row1 = row1iter.hasNext()?row1iter.next():Integer.MAX_VALUE;
				} else {
					// row2 < row1
					row2 = row2iter.hasNext()?row2iter.next():Integer.MAX_VALUE;
				}
			}
		}
	}
	
	@Override
	public void visit(OrExpression or) {
		or.getLeftExpression().accept(this);
		or.getRightExpression().accept(this);
		// Merge sorted row index lists
		List<Integer> rows1 = qualifyingRows.pop();
		List<Integer> rows2 = qualifyingRows.pop();
		if (rows1.isEmpty()) {
			qualifyingRows.push(rows2);
		} else if (rows2.isEmpty()) {
			qualifyingRows.push(rows1);
		} else {
			// Both input lists are non-empty
			List<Integer> mergedRows = new ArrayList<Integer>();
			qualifyingRows.push(mergedRows);
			Iterator<Integer> row1iter = rows1.iterator();
			Iterator<Integer> row2iter = rows2.iterator();
			int row1 = row1iter.next();
			int row2 = row2iter.next();
			// Assume that tables contain at most Integer.MAX_VALUE - 1 elements
			while (row1 != Integer.MAX_VALUE || 
					row2 != Integer.MAX_VALUE) {
				if (row1 < row2) {
					mergedRows.add(row1);
					row1 = row1iter.hasNext()?row1iter.next():Integer.MAX_VALUE;
				} else if (row2 < row1) {
					mergedRows.add(row2);
					row2 = row2iter.hasNext()?row2iter.next():Integer.MAX_VALUE;
				} else {
					// row1 == row2
					mergedRows.add(row1);
					if (row1iter.hasNext()) {
						row1 = row1iter.next();
					} else {
						row2 = row2iter.next();
					}
				}
			}
		}
	}
	
	@Override
	public void visit(EqualsTo equalsTo) {
		equalsTo.getLeftExpression().accept(this);
		equalsTo.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		Index index = applicableIndices.pop();
		// Collect indices of satisfying rows via index
		List<Integer> rows = new ArrayList<Integer>();
		qualifyingRows.push(rows);
		ThreadIntIndex intIndex = (ThreadIntIndex)index;
		int startPos = intIndex.keyToPositions.getOrDefault(constant, -1);
		if (startPos >= 0) {
			int[] array = intIndex.newPositions[startPos];
			for (int element: array) {
				rows.add(element);
			}
//			rows.addAll(intIndex.newPositions.get(startPos));
//			startPos = intIndex.posList.get(startPos);
//			int nrEntries = intIndex.positions[startPos];
//			for (int i=0; i<nrEntries; ++i) {
//				int pos = startPos + 1 + i;
//				int rowIdx = intIndex.positions[pos];
//				rows.add(rowIdx);
//			}
		}
	}
	@Override
	public void visit(NotEqualsTo notEqualsTo) {
		notEqualsTo.getLeftExpression().accept(this);
		notEqualsTo.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		Index index = applicableIndices.pop();
		// Collect indices of satisfying rows via index
		List<Integer> rows = new ArrayList<>();
		qualifyingRows.push(rows);
		ThreadIntIndex intIndex = (ThreadIntIndex)index;
		int startPos = intIndex.keyToPositions.getOrDefault(constant, -1);
		int start = 0;
		if (startPos >= 0) {
//			startPos = intIndex.posList.get(startPos);
			int nrEntries = intIndex.positions[startPos];
			for (int i = 0; i < nrEntries; ++i) {
				int pos = startPos + 1 + i;
				int rowIdx = intIndex.positions[pos];
				rows.addAll(IntStream.range(start, rowIdx).boxed()
						.collect(Collectors.toList()));
				start = rowIdx + 1;
			}
		}
	}

	@Override
	public void visit(GreaterThanEquals greaterThanEquals) {
		greaterThanEquals.getLeftExpression().accept(this);
		greaterThanEquals.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		ThreadIntIndex intIndex = (ThreadIntIndex)applicableIndices.pop();
		// Collect indices of satisfying rows via index
		Set<Integer> values = intIndex.distinctValues.parallelStream().filter(value -> value >= constant).collect(Collectors.toSet());
		int cardinality = intIndex.cardinality;
		// Initialize filter result
		List<Integer> rows;
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			rows = filterBatch(intIndex, allTuples, values);
		} else {
			// Divide tuples into batches
			List<RowRange> batches = split(cardinality);
			// Process batches in parallel
			rows = batches.parallelStream().flatMap(batch ->
					filterBatch(intIndex, batch, values).stream()).collect(
					Collectors.toList());
		}
		qualifyingRows.push(rows);
	}

	@Override
	public void visit(GreaterThan greaterThan) {
		greaterThan.getLeftExpression().accept(this);
		greaterThan.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		ThreadIntIndex intIndex = (ThreadIntIndex)applicableIndices.pop();
		// Collect indices of satisfying rows via index
		Set<Integer> values = intIndex.distinctValues.parallelStream().filter(value -> value > constant).collect(Collectors.toSet());
		int cardinality = intIndex.cardinality;
		// Initialize filter result
		List<Integer> rows;
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			rows = filterBatch(intIndex, allTuples, values);
		} else {
			// Divide tuples into batches
			List<RowRange> batches = split(cardinality);
			// Process batches in parallel
			rows = batches.parallelStream().flatMap(batch ->
					filterBatch(intIndex, batch, values).stream()).collect(
					Collectors.toList());
		}
		qualifyingRows.push(rows);
	}
	@Override
	public void visit(MinorThanEquals minorThanEquals) {
		minorThanEquals.getLeftExpression().accept(this);
		minorThanEquals.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		ThreadIntIndex intIndex = (ThreadIntIndex)applicableIndices.pop();
		// Collect indices of satisfying rows via index
		Set<Integer> values = intIndex.distinctValues.parallelStream().filter(value -> value <= constant).collect(Collectors.toSet());
		int cardinality = intIndex.cardinality;
		// Initialize filter result
		List<Integer> rows;
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			rows = filterBatch(intIndex, allTuples, values);
		} else {
			// Divide tuples into batches
			List<RowRange> batches = split(cardinality);
			// Process batches in parallel
			rows = batches.parallelStream().flatMap(batch ->
					filterBatch(intIndex, batch, values).stream()).collect(
					Collectors.toList());
		}
		qualifyingRows.push(rows);
	}
	@Override
	public void visit(MinorThan minorThan) {
		minorThan.getLeftExpression().accept(this);
		minorThan.getRightExpression().accept(this);
		// We assume predicate passed the index test so
		// there must be one index and one constant.
		int constant = extractedConstants.pop();
		ThreadIntIndex intIndex = (ThreadIntIndex)applicableIndices.pop();
		// Collect indices of satisfying rows via index
		Set<Integer> values = intIndex.distinctValues.parallelStream().filter(value -> value < constant).collect(Collectors.toSet());
		int cardinality = intIndex.cardinality;
		// Initialize filter result
		List<Integer> rows;
		if (cardinality <= ParallelConfig.PRE_BATCH_SIZE) {
			RowRange allTuples = new RowRange(0, cardinality - 1);
			rows = filterBatch(intIndex, allTuples, values);
		} else {
			// Divide tuples into batches
			List<RowRange> batches = split(cardinality);
			// Process batches in parallel
			rows = batches.parallelStream().flatMap(batch ->
					filterBatch(intIndex, batch, values).stream()).collect(
					Collectors.toList());
		}
		qualifyingRows.push(rows);
	}

	/**
	 * Splits table with given cardinality into tuple batches
	 * according to the configuration for parallel processing.
	 *
	 * @param cardinality	cardinality of table to split
	 * @return				list of row ranges (batches)
	 */
	static List<RowRange> split(int cardinality) {
		List<RowRange> batches = new ArrayList<>();
		for (int batchCtr=0; batchCtr*ParallelConfig.PRE_BATCH_SIZE<cardinality;
			 ++batchCtr) {
			int startIdx = batchCtr * ParallelConfig.PRE_BATCH_SIZE;
			int tentativeEndIdx = startIdx + ParallelConfig.PRE_BATCH_SIZE-1;
			int endIdx = Math.min(cardinality - 1, tentativeEndIdx);
			RowRange rowRange = new RowRange(startIdx, endIdx);
			batches.add(rowRange);
		}
		return batches;
	}

	/**
	 * Filters given tuple batch using specified predicate evaluator,
	 * return indices of rows within the batch that satisfy the
	 * predicate.
	 *
	 * @param rowRange		range of tuple indices of batch
	 * @return				list of indices satisfying the predicate
	 */
	static List<Integer> filterBatch(ThreadIntIndex intIndex, RowRange rowRange, Set<Integer> values) {
		List<Integer> result = new ArrayList<>();
		// Evaluate predicate for each table row
		for (int rowCtr=rowRange.firstTuple;
			 rowCtr<=rowRange.lastTuple; ++rowCtr) {
			int data = intIndex.intData.data[rowCtr];
			if (values.contains(data)) {
				result.add(rowCtr);
			}
		}
		return result;
	}
	
	@Override
	public void visit(LongValue longValue) {
		extractedConstants.push((int)longValue.getValue());
	}
	
	@Override
	public void visit(StringValue stringValue) {
		// String must be in dictionary due to index test
		String strVal = stringValue.getValue();
		int code = BufferManager.dictionary.getCode(strVal);
		extractedConstants.push(code);
	}
	
	@Override
	public void visit(Column column) {
		// Resolve column reference
		String aliasName = column.getTable().getName();
		String tableName = query.aliasToTable.get(aliasName);
		String columnName = column.getColumnName();
		ColumnRef colRef = new ColumnRef(tableName, columnName);
		// Check for available index
		Index index = BufferManager.colToIndex.get(colRef);
		if (index != null) {
			applicableIndices.push(index);
		}
	}
}
