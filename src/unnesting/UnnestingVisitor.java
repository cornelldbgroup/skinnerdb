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
import config.NamingConfig;
import expressions.normalization.CopyVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
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
public class UnnestingVisitor extends CopyVisitor implements SelectVisitor {
	/**
	 * Used to assign unique IDs to anonymous sub-queries.
	 */
	public int nextSubqueryID = 0;
	/**
	 * Used to assign unique IDs to anonymous attributes.
	 */
	public int nextAttributeID = 0;
	/**
	 * Sequence of simple queries (i.e., without nested queries)
	 * such that processing queries in this order satisfies all
	 * dependencies.
	 */
	public List<PlainSelect> unnestedQueries = new ArrayList<>();
	/**
	 * Contans available column references in outer query scope
	 * from the perspective of current sub-query on top.
	 */
	public Stack<Set<ColumnRef>> outerCols = new Stack<>();
	/**
	 * Maps aliases in FROM clause to associated column names,
	 * top element represents current sub-query.
	 */
	public Stack<Map<String, List<String>>> aliasToCols = new Stack<>();
	/**
	 * Contains the names of result fields of the last treated
	 * sub-query on top.
	 */
	public Stack<List<String>> subqueryFields = new Stack<>();
	/**
	 * List of references to tables containing sub-query results
	 * that need to be added to FROM clause.
	 */
	public Stack<Table> addToFrom = new Stack<>();
	/**
	 * List of correlated predicates that need to be added to
	 * WHERE clause (since they cannot be resolved in inner
	 * scope).
	 */
	public Stack<Expression> addToWhere = new Stack<>();
	/**
	 * Updates current scope and alias-to-columns mapping
	 * based on base tables that appear in FROM clause of
	 * given query.
	 * 
	 * @param plainSelect	extract columns for this query's FROM clause
	 */
	void treatSimpleFrom(PlainSelect plainSelect) {
		// Retrieve fields to update
		Set<ColumnRef> curScopeCols = outerCols.peek();
		Map<String, List<String>> curAliasToCols = aliasToCols.peek();
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
				// Update scope and mappings
				List<String> curAliasCols = new ArrayList<>();
				curAliasToCols.put(alias, curAliasCols);
				for (ColumnInfo colInfo : tableInfo.nameToCol.values()) {
					String colName = colInfo.name;
					// Update current scope
					curScopeCols.add(new ColumnRef("", colName));
					curScopeCols.add(new ColumnRef(alias, colName));
					// Update current alias to column mapping
					curAliasCols.add(colName);
				}
			}
		}
	}
	/**
	 * Unnests one single from item and returns unnested version.
	 * Also updates the current column scope by adding references
	 * to sub-query result columns and alias-to-column mapping.
	 * 
	 * @param fromItem		original from item to unnest
	 * @param scopeCols		available columns in current scope
	 * @param aliasToCols	maps aliases to associated columns
	 * @return				updated from item after unnesting
	 */
	FromItem unnestFromItem(FromItem fromItem, Set<ColumnRef> scopeCols,
			Map<String, List<String>> aliasToCols) {
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
				// Update scope and column mapping
				List<String> newCols = subqueryFields.pop();
				for (String col : newCols) {
					scopeCols.add(new ColumnRef("", col));
					scopeCols.add(new ColumnRef(alias, col));					
				}
				aliasToCols.put(alias, newCols);
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
	 */
	void treatNestedFrom(PlainSelect plainSelect) {
		// Retrieve current scope
		Set<ColumnRef> curScopeCols = outerCols.peek();
		Map<String, List<String>> curAliasToCols = aliasToCols.peek();
		// Update first item in FROM clause
		plainSelect.setFromItem(unnestFromItem(
				plainSelect.getFromItem(), 
				curScopeCols, curAliasToCols));
		// Update remaining items in FROM clause
		List<Join> joins = plainSelect.getJoins();
		if (joins != null) {
			for (Join join : joins) {
				join.setRightItem(unnestFromItem(
						join.getRightItem(), 
						curScopeCols, curAliasToCols));
			}
		}
	}
	/**
	 * Resolve wildcards in select clause, replacing them
	 * by references to concrete columns of base tables or
	 * nested queries.
	 * 
	 * @param plainSelect	replace wildcards in this query's select clause
	 */
	void resolveWildcards(PlainSelect plainSelect) {
		Map<String, List<String>> curAliasToCols = aliasToCols.peek();
		List<SelectItem> originalItems = plainSelect.getSelectItems();
		// Remove all columns wild card
		List<SelectItem> noAllColumns = new ArrayList<>();
		for (SelectItem originalItem : originalItems) {
			if (originalItem instanceof AllColumns) {
				for (String alias : curAliasToCols.keySet()) {
					Table aliasTbl = new Table(alias);
					AllTableColumns allTableCols = 
							new AllTableColumns(aliasTbl);
					noAllColumns.add(allTableCols);
				}
			} else {
				noAllColumns.add(originalItem);
			}
		}
		// Remove all table columns wild cards
		List<SelectItem> noAllTblCols = new ArrayList<>();
		for (SelectItem curItem : noAllColumns) {
			if (curItem instanceof AllTableColumns) {
				AllTableColumns allTblCols = (AllTableColumns)curItem;
				Table table = allTblCols.getTable();
				String tblName = table.getName();
				for (String columnName : curAliasToCols.get(tblName)) {
					Column col = new Column(table, columnName);
					noAllTblCols.add(new SelectExpressionItem(col));
				}
			} else {
				noAllTblCols.add(curItem);
			}
		}
		plainSelect.setSelectItems(noAllTblCols);
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
			List<String> selectNames = new ArrayList<>();
			for (SelectItem selectItem : selectItems) {
				Expression selectExpr = ((SelectExpressionItem)
						selectItem).getExpression();
				selectNames.add(selectToName.get(selectExpr));
			}
			subqueryFields.add(selectNames);
		} catch (SQLexception e) {
			sqlExceptions.add(e);
		}
	}
	/**
	 * Expand FROM clause by adding tables containing
	 * results of nested queries that were rewritten
	 * during unnesting.
	 * 
	 * @param plainSelect	expand this query's FROM clause
	 */
	void expandFrom(PlainSelect plainSelect) {
		while (!addToFrom.isEmpty()) {
			Table toAdd = addToFrom.pop();
			FromItem firstItem = plainSelect.getFromItem();
			if (firstItem == null) {
				plainSelect.setFromItem(toAdd);
			} else {
				// Make sure that join list is initialized
				List<Join> joins = plainSelect.getJoins();
				if (joins == null) {
					joins = new ArrayList<>();
					plainSelect.setJoins(joins);
				}
				// Add new table via simple join
				Join join = new Join();
				join.setSimple(true);
				join.setRightItem(toAdd);
				joins.add(join);
			}
		}
	}
	
	@Override
	public void visit(PlainSelect plainSelect) {
		// Initialize new scope and new alias-to-column mapping
		Set<ColumnRef> newOuterCols = new HashSet<>();
		newOuterCols.addAll(outerCols.peek());
		outerCols.add(newOuterCols);
		Map<String, List<String>> curAliasToCols = new HashMap<>();
		aliasToCols.add(curAliasToCols);
		// Treat base tables in FROM clause
		treatSimpleFrom(plainSelect);
		// Treat sub-queries in FROM clause
		treatNestedFrom(plainSelect);
		// Resolve wildcard in SELECT clause if any
		resolveWildcards(plainSelect);
		// Unnest sub-queries in WHERE clause
		Expression originalWhere = plainSelect.getWhere();
		if (originalWhere != null) {
			// Replace where clause by unnested version
			originalWhere.accept(this);
			Expression newWhere = exprStack.pop();
			plainSelect.setWhere(newWhere);
		}
		// Add unnested sub-queries to FROM clause if any
		expandFrom(plainSelect);
		
		
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
	/**
	 * This method is invoked for subselects in
	 * where and select clauses while subselects
	 * in the FROM clause are treated separately.
	 */
	@Override
	public void visit(SubSelect subSelect) {
		// Check for alias - should not have any
		if (subSelect.getAlias() != null) {
			sqlExceptions.add(new SQLexception("Error -"
					+ "specified alias for anonymous "
					+ "sub-query: " + subSelect));
		} else {
			// Name anonymous sub-query
			String alias = NamingConfig.SUBQUERY_PRE + nextSubqueryID;
			++nextSubqueryID;
			// Unnest nested sub-query if possible
			SelectBody selectBody = subSelect.getSelectBody();
			if (selectBody instanceof PlainSelect) {
				PlainSelect plainSelect = (PlainSelect)selectBody;
				// Rewrite sub-query and add to query list
				plainSelect.accept(this);
				Table resultTable = new Table(alias);
				plainSelect.setIntoTables(Arrays.asList(
						new Table[] {resultTable}));
				// Replace nested sub-query by table reference
				List<String> subqueryCols = subqueryFields.pop();
				String firstCol = subqueryCols.get(0);
				exprStack.add(new Column(resultTable, firstCol));
				// Schedule table containing sub-query result to 
				// be added to FROM clause.
				addToFrom.add(resultTable);
			} else {
				sqlExceptions.add(new SQLexception("Error - "
						+ "unsupported sub-query type: "
						+ selectBody + " (type: " + 
						selectBody.getClass() + ")"));
			}
		}
	}	
}
