package unnesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import expressions.normalization.PlainVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import query.ColumnRef;
import query.SQLexception;
import query.from.FromUtil;
import query.select.SelectUtil;

/**
 * Decomposes a query that may contain nested (potentially
 * correlated) sub-queries into a sequence of simple queries.
 * 
 * @author immanueltrummer
 *
 */
public class UnnestingVisitor extends PlainVisitor implements SelectVisitor {
	/**
	 * Used to assign unique IDs to anonymous sub-queries.
	 */
	public int nextSubqueryID = 0;
	/**
	 * Used to assign unique IDs to anonymous attributes.
	 */
	public int nextAttributeID = 0;
	/**
	 * Contans available column references in outer query scope
	 * from the perspective of current sub-query on top.
	 */
	public Stack<Set<ColumnRef>> outerCols = new Stack<>();
	/**
	 * Sequence of simple queries (i.e., without nested queries)
	 * such that processing queries in this order satisfies all
	 * dependencies.
	 */
	public List<PlainSelect> unnestedQueries = new ArrayList<>();
	/**
	 * Maps sub-queries to names of associated result tables.
	 */
	public Map<PlainSelect, String> queryNames = new HashMap<>();
	/**
	 * Contains the names of result fields of the last treated
	 * sub-query on top.
	 */
	public Stack<Set<String>> subqueryFields = new Stack<>();
	/**
	 * Extracts all possible column references to base tables
	 * that appear in the given query's FROM clause (note: we
	 * do not check whether the column references are unique
	 * so this returns a superset of the column references that
	 * can be used in queries).
	 * 
	 * @param plainSelect	extract columns for this query's FROM clause
	 * @return				column references to base tables in FROM clause
	 */
	Set<ColumnRef> baseTableCols(PlainSelect plainSelect) {
		// This will become the result set
		Set<ColumnRef> baseTableCols = new HashSet<>();
		// Get all items in FROM clause
		List<FromItem> fromItems = FromUtil.allFromItems(plainSelect);
		// Iterate over base tables in FROM clause
		for (FromItem fromItem : fromItems) {
			if (fromItem instanceof Table) {
				// Extract table and alias name (defaults to table name)
				Table table = (Table)fromItem;
				String tableName = table.getName().toLowerCase();
				String alias = table.getAlias()!=null?
						table.getAlias().getName():tableName;
				// Extract associated column references
				TableInfo tableInfo = CatalogManager.currentDB.
						nameToTable.get(tableName);
				for (ColumnInfo colInfo : tableInfo.nameToCol.values()) {
					String colName = colInfo.name;
					baseTableCols.add(new ColumnRef("", colName));
					baseTableCols.add(new ColumnRef(alias, colName));
				}
			}
		}
		return baseTableCols;
	}
	/**
	 * Unnests one single from item and returns unnested version.
	 * Also updates the current column scope by adding references
	 * to sub-query result columns.
	 * 
	 * @param fromItem		original from item to unnest
	 * @param scopeCols		available columns in current scope
	 * @return				updated from item after unnesting
	 */
	FromItem unnestFromItem(FromItem fromItem, Set<ColumnRef> scopeCols) {
		// Does from item need unnesting?
		if (fromItem instanceof SubSelect) {
			SubSelect subSelect = (SubSelect)fromItem;
			String alias = subSelect.getAlias().getName().toLowerCase();
			SelectBody selectBody = subSelect.getSelectBody();
			if (selectBody instanceof PlainSelect) {
				// Recursively unnest sub-query
				selectBody.accept(this);
				// Sub-query will be replaced by table reference
				Table table = new Table(alias);
				// Associate unnested sub-query with name
				PlainSelect plainSelect = (PlainSelect)selectBody;
				plainSelect.setIntoTables(Arrays.asList(
						new Table[] {table}));
				queryNames.put(plainSelect, alias);
				// Add references to sub-query columns to scope
				Set<String> newCols = subqueryFields.pop();
				for (String col : newCols) {
					scopeCols.add(new ColumnRef("", col));
					scopeCols.add(new ColumnRef(alias, col));					
				}
				// Return table that will contain sub-query result
				return (FromItem)table;
			} else {
				// (This case raises an exception before)
				return fromItem;
			}
		} else {
			// No changes if no nested query
			return fromItem;
		}
	}
	/**
	 * Unnest items in FROM clause and add result fields
	 * to available column references in current scope.
	 * 
	 * @param plainSelect	query whose FROM clause is unnested
	 * @param scopeCols		available columns in current scope
	 */
	void unnestFrom(PlainSelect plainSelect, Set<ColumnRef> scopeCols) {
		// Update first item in FROM clause
		plainSelect.setFromItem(unnestFromItem(
				plainSelect.getFromItem(), scopeCols));
		// Update remaining items in FROM clause
		List<Join> joins = plainSelect.getJoins();
		if (joins != null) {
			for (Join join : joins) {
				join.setRightItem(unnestFromItem(
						join.getRightItem(), scopeCols));
			}
		}
	}
	/**
	 * Register names of result columns for this query
	 * (used to resolve column references in containing
	 * queries).
	 * 
	 * @param plainSelect	query for which to register result
	 */
	void registerResultCols(PlainSelect plainSelect) {
		try {
			List<SelectItem> selectItems = plainSelect.getSelectItems();
			Map<Expression, String> selectToName = 
					SelectUtil.assignAliases(selectItems);
			Set<String> selectNames = new HashSet<>();
			selectNames.addAll(selectToName.values());
			subqueryFields.add(selectNames);
		} catch (SQLexception e) {
			sqlExceptions.add(e);
		}
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		// Collect column references from base tables in FROM clause
		Set<ColumnRef> baseCols = baseTableCols(plainSelect);
		// Register new outer scope for all nested queries
		Set<ColumnRef> newOuterCols = new HashSet<>();
		newOuterCols.addAll(baseCols);
		newOuterCols.addAll(outerCols.peek());
		outerCols.add(newOuterCols);
		// Treat sub-queries in FROM clause
		unnestFrom(plainSelect, newOuterCols);
		// Unnest sub-queries in WHERE clause
		// Add unnested sub-queries to FROM clause if any
		// Resolve wildcard in SELECT clause
		// Add fields required by outer scope to SELECT clause
		// Add unnested query to query sequence
		unnestedQueries.add(plainSelect);
		// Register names of sub-query result fields
		registerResultCols(plainSelect);
		// Remove new outer scope
		outerCols.pop();
	}

	@Override
	public void visit(SetOperationList setOpList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithItem withItem) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub
		
	}	
}
