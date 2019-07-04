package operators;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import buffer.BufferManager;
import expressions.normalization.PlainVisitor;
import indexing.Index;
import indexing.IntIndex;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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
		IntIndex intIndex = (IntIndex)index;
		int startPos = intIndex.keyToPositions.getOrDefault(constant, -1);
		if (startPos >= 0) {
			int nrEntries = intIndex.positions[startPos];
			for (int i=0; i<nrEntries; ++i) {
				int pos = startPos + 1 + i;
				int rowIdx = intIndex.positions[pos];
				rows.add(rowIdx);
			}				
		}
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
