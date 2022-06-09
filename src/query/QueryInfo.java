package query;

import java.util.*;
import java.util.Map.Entry;

import buffer.BufferManager;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.LoggingConfig;
import config.NamingConfig;
import expressions.ExpressionInfo;
import expressions.aggregates.AggInfo;
import expressions.typing.ExpressionScope;
import indexing.Index;
import joining.parallel.indexing.DoublePartitionIndex;
import joining.parallel.indexing.IntPartitionIndex;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.cnfexpression.CNFConverter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import predicate.NonEquiCols;
import predicate.NonEquiNode;
import predicate.NonEquiNodesTest;
import preprocessing.Context;
import query.from.FromUtil;
import query.select.SelectUtil;

import org.apache.commons.lang3.StringUtils;

/**
 * Contains information on the query to execute.
 * 
 * @author Anonymous
 *
 */
public class QueryInfo {
	/**
	 * Plain select statement query to execute.
	 */
	public final PlainSelect plainSelect;
	/**
	 * Whether the query execution should be visualized.
	 */
	public final boolean explain;
	/**
	 * Plot at most that many plots.
	 */
	public final int plotAtMost;
	/**
	 * Generate a plot every so many samples.
	 */
	public final int plotEvery;
	/**
	 * Generate plots in this directory if activated.
	 */
	public final String plotDir;
	/**
	 * Number of table instances in FROM clause.
	 */
	public int nrJoined = 0;
	/**
	 * All aliases in the query's FROM clause.
	 */
	public String[] aliases;
	/**
	 * Maps each alias to its alias index.
	 */
	public Map<String, Integer> aliasToIndex =
			new HashMap<String, Integer>();
	/**
	 * Maps aliases to associated table name.
	 */
	public Map<String, String> aliasToTable = 
			new HashMap<String, String>();
	/**
	 * Maps unique column names to associated table aliases.
	 */
	public Map<String, String> columnToAlias =
			new HashMap<String, String>();
	/**
	 * Maps from aliases to SQL expressions.
	 */
	public Map<String, Expression> aliasToExpression =
			new HashMap<String, Expression>();
	/**
	 * Maps select clause items to corresponding alias.
	 */
	public Map<ExpressionInfo, String> selectToAlias =
			new HashMap<>();
	/**
	 * Maps column reference to column info.
	 */
	public Map<ColumnRef, ColumnInfo> colRefToInfo =
			new HashMap<ColumnRef, ColumnInfo>();
	/**
	 * Expressions that appear in the SELECT clause
	 * with associated meta-data.
	 */
	public List<ExpressionInfo> selectExpressions =
			new ArrayList<ExpressionInfo>();
	/**
	 * Stores information on predicates in WHERE clause.
	 * Each expression integrates all predicates that
	 * refer to the same table instances.
	 */
	public List<ExpressionInfo> wherePredicates = 
			new ArrayList<ExpressionInfo>();
	/**
	 * List of expressions that correspond to unary
	 * predicates.
	 */
	public List<ExpressionInfo> unaryPredicates =
			new ArrayList<ExpressionInfo>();
	/**
	 * Sets of alias indices that are connected via
	 * join predicates - used to quickly determined
	 * eligible tables for continuing join orders
	 * while avoiding Cartesian product joins.
	 */
	public Set<Set<Integer>> joinedIndices = new HashSet<>();
	/**
	 * Map alias indices that are connected via
	 * join predicates to selectivity.
	 */
	public Map<Integer, Double> joinedSelectivity = new HashMap<>();
	/**
	 * Columns that are involved in binary equi-join
	 * predicates (i.e., we may want to create hash
	 * indices for them during pre-processing).
	 */
	public Set<ColumnRef> equiJoinCols = new HashSet<>();
	/**
	 * Columns that are needed to create an index.
	 */
	public Set<ColumnRef> indexCols = new HashSet<>();
	/**
	 * List of expressions that correspond to
	 * equality join predicates.
	 */
	public List<ExpressionInfo> equiJoinPreds = 
			new ArrayList<>();
	/**
	 * List of expressions that correspond to
	 * non-equi join predicates.
	 */	
	public List<ExpressionInfo> nonEquiJoinPreds =
			new ArrayList<ExpressionInfo>();
	/**
	 * List of trees that correspond to
	 * non-equi join predicates.
	 */
	public List<NonEquiNode> nonEquiJoinNodes =
			new ArrayList<>();
	/**
	 * Expressions that appear in GROUP-BY clause with
	 * associated meta-data.
	 */
	public List<ExpressionInfo> groupByExpressions =
			new ArrayList<ExpressionInfo>();
	/**
	 * Expressions that appear in ORDER-BY clause with
	 * associated meta-data.
	 */
	public List<ExpressionInfo> orderByExpressions =
			new ArrayList<ExpressionInfo>();
	/**
	 * Whether we sort i-th order-by element in
	 * ascending (as opposed to descending) order.
	 */
	public boolean[] orderByAsc;
	/**
	 * HAVING clause expression with meta-data.
	 */
	public ExpressionInfo havingExpression;
	/**
	 * Set of columns required for join processing.
	 */
	public Set<ColumnRef> colsForJoins =
			new HashSet<ColumnRef>();
	/**
	 * Set of columns required for post-processing.
	 */
	public Set<ColumnRef> colsForPostProcessing =
			new HashSet<ColumnRef>();
	/**
	 * Aggregate expressions in SELECT clause.
	 */
	public Set<AggInfo> aggregates = new HashSet<>();
	/**
	 * Class of aggregation query.
	 */
	public final AggregationType aggregationType;
	/**
	 * Number of tuples specified in LIMIT clause, if any,
	 * or -1 if no LIMIT specified.
	 */
	public final int limit;
	/**
	 * The set of temporary alias that are the result of
	 * inner sub query.
	 */
	public final Set<String> temporaryAlias = new HashSet<>();
	/**
	 * The set of temporary tables that are the result of
	 * inner sub query.
	 */
	public final Set<Integer> temporaryTables = new HashSet<>();
	/**
	 * The set of temporary tables that are the result of
	 * inner sub query.
	 */
	public final Map<Integer, Integer> temporaryConnection = new HashMap<>();
	/**
	 * For each table, initialize a set of tables that connect to the key.
	 */
	public final Map<Integer, Set<Integer>> joinConnection = new HashMap<>();
	/**
	 * A set of potential constraints of join order.
	 */
	public final Set<Pair<Integer, Integer>> constraints = new HashSet<>();
	/**
	 * A temporary position data structures for the sub-query.
	 */
	public final Map<ColumnRef, Deque<int[]>> aliasToPositions =
			new HashMap<>();

	/**
	 * Extract information from the FROM clause (e.g.,
	 * all tables referenced with their aliases, the
	 * number of items in the from clause etc.).
	 */
	void extractFromInfo() throws Exception {
		// Check if FROM clause exists
		if (plainSelect.getFromItem() == null) {
			throw new SQLexception("Error - no FROM clause");
		}
		// Extract all from items
		List<FromItem> fromItems = FromUtil.allFromItems(plainSelect);
		nrJoined = fromItems.size();
		// Extract tables from items
		aliases = new String[nrJoined];
		for (int i=0; i<nrJoined; ++i) {
			FromItem fromItem = fromItems.get(i);
			// Retrieve information on associated table
			Table table = (Table)fromItem;
			String alias = table.getAlias()!=null?
					table.getAlias().getName().toLowerCase():
						table.getName().toLowerCase();
			String tableName = table.getName().toLowerCase();
			// Verify that table is known
			if (!CatalogManager.currentDB.
					nameToTable.containsKey(tableName)) {
				throw new SQLexception("Error - table " + 
						tableName + " is unknown");
			}
			// Verify that alias is unique
			for (int j=0; j<i; ++j) {
				if (aliases[j].equals(alias)) {
					throw new SQLexception("Error - "
							+ "alias " + alias + " is "
									+ "not unique");
				}
			}
			TableInfo tableInfo = CatalogManager.currentDB.
					nameToTable.get(tableName);
			// Register mapping from alias to table
			aliasToTable.put(alias, tableName);
			// Register mapping from index to alias
			aliases[i] = alias;
			// Register mapping from alias to index
			aliasToIndex.put(alias, i);
			// Extract columns with types
			for (ColumnInfo colInfo : tableInfo.nameToCol.values()) {
				String colName = colInfo.name;
				colRefToInfo.put(new ColumnRef(alias, colName), colInfo);
			}
		}
	}
	/**
	 * Adds implicit table references via unique column names.
	 */
	void addImplicitRefs() throws Exception {
		for (Entry<String, String> entry : aliasToTable.entrySet()) {
			String alias = entry.getKey();
			String table = entry.getValue();
			// Disallow implicit references for tables added during
			// unnesting to represent the results of sub-queries.
			if (!table.startsWith(NamingConfig.SUBQUERY_PRE)) {
				for (ColumnInfo columnInfo : CatalogManager.currentDB.
						nameToTable.get(table).nameToCol.values()) {
					String columnName = columnInfo.name;
					if (columnToAlias.containsKey(columnName)) {
						columnToAlias.put(columnName, null);
					} else {
						columnToAlias.put(columnName, alias);
					}
				}				
			}
		}
	}
	/**
	 * Adds all columns of a given table to a list of select clause items.
	 * 
	 * @param tblAlias		add columns for this table alias
	 * @param selectItems	add columns to this select item list
	 */
	void addAllColumns(String tblAlias, List<SelectItem> selectItems) {
		String tableName = aliasToTable.get(tblAlias);
		TableInfo tblInfo = CatalogManager.currentDB.nameToTable.get(tableName);
		Table table = new Table(tblAlias);
		for (String colName : tblInfo.columnNames) {
			Column column = new Column(table, colName);
			selectItems.add(new SelectExpressionItem(column));
		}
	}
	/**
	 * Resolve wildcards in SELECT clause and assign aliases.
	 */
	void treatSelectClause() throws Exception {
		// Expand SELECT clause into list of simple select items
		// (i.e., resolve all wildcards).
		List<SelectItem> selectItems = new ArrayList<>();
		// Expand SELECT clause into list of expressions
		for (SelectItem selectItem : plainSelect.getSelectItems()) {
			if (selectItem instanceof SelectExpressionItem) {
				selectItems.add(selectItem);
			} else if (selectItem instanceof AllTableColumns) {
				AllTableColumns allTblCols = (AllTableColumns)selectItem;
				String aliasName = allTblCols.getTable().getName();
				addAllColumns(aliasName, selectItems);
			} else if (selectItem instanceof AllColumns) {
				for (String tblAlias : aliases) {
					addAllColumns(tblAlias, selectItems);
				}
			} else {
				System.out.println("Unknown type of select "
						+ "clause item - ignoring");
			}
		}
		// Name items in select clause
		Map<Expression, String> selectExprToAlias = 
				SelectUtil.assignAliases(selectItems);
		// Update fields associated with select clause
		for (SelectItem selectItem : selectItems) {
			SelectExpressionItem exprItem = (SelectExpressionItem)selectItem;
			Expression expr = exprItem.getExpression();
			String alias = selectExprToAlias.get(expr);
			ExpressionInfo exprInfo = new ExpressionInfo(this, expr);
			selectExpressions.add(exprInfo);
			aliasToExpression.put(alias, exprInfo.finalExpression);
			selectToAlias.put(exprInfo, alias);
		}
	}
	/**
	 * Assuming the given expression is a binary equality join,
	 * returns the two joined columns. Returns null otherwise.
	 * 
	 * @param exprInfo	potential equi-join predicate
	 */
	Set<ColumnRef> extractEquiJoinCols(ExpressionInfo exprInfo) {
		Expression expr = exprInfo.finalExpression;
		// Check for equality predicate
		if (expr instanceof EqualsTo) {
			EqualsTo equalsExpr = (EqualsTo)expr;
			Expression left = equalsExpr.getLeftExpression();
			Expression right = equalsExpr.getRightExpression();
			// Is it an equality between two columns?
			if (left instanceof Column && right instanceof Column && !equalsExpr.isNot()) {
				Column leftCol = (Column)left;
				Column rightCol = (Column)right;
				ColumnRef leftRef = new ColumnRef(
						leftCol.getTable().getName(),
						leftCol.getColumnName());
				ColumnRef rightRef = new ColumnRef(
						rightCol.getTable().getName(),
						rightCol.getColumnName());

				int leftIndex = aliasToIndex.get(leftRef.aliasName);
				int rightIndex = aliasToIndex.get(rightRef.aliasName);
				joinConnection.get(leftIndex).add(rightIndex);
				joinConnection.get(rightIndex).add(leftIndex);

				// Do those columns belong to different tables?
				if (!leftRef.aliasName.equals(rightRef.aliasName)) {
					Set<ColumnRef> colPair = new HashSet<>();
					colPair.add(leftRef);
					colPair.add(rightRef);
					return colPair;					
				}
			}
		}
		return null;
	}

	Set<ColumnRef> extractNonEquiJoinCols(ExpressionInfo exprInfo) {
		NonEquiCols nonEquiCols = new NonEquiCols(this);
		exprInfo.conjuncts.get(0).accept(nonEquiCols);
		return nonEquiCols.extractedCols;
	}


	/**
	 * Extracts predicates from normalized WHERE clause, separating
	 * predicates by the tables they refer to.
	 */
	void extractPredicates() throws Exception {
		// initialize join connection
		for (int i = 0; i < nrJoined; i++) {
			joinConnection.put(i, new HashSet<>());
		}
		Expression where = plainSelect.getWhere();
		if (where != null) {
			// Normalize WHERE clause and transform into CNF
			ExpressionInfo whereInfo = new ExpressionInfo(this, where);
			// Decompose into conjuncts
			List<Expression> conjuncts = whereInfo.conjuncts;
			// Merge conditions that refer to the same tables
			Map<Set<String>, Expression> tablesToCondition = 
					new HashMap<Set<String>, Expression>();
			for (Expression conjunct : conjuncts) {
				ExpressionInfo conjunctInfo = 
						new ExpressionInfo(this, conjunct);
				Set<String> tables = conjunctInfo.aliasesMentioned;
				if (tablesToCondition.containsKey(tables)) {
					Expression prior = tablesToCondition.get(tables);
					Expression curAndPrior = 
							new AndExpression(prior, conjunct);
					tablesToCondition.put(tables, curAndPrior);
				} else {
					tablesToCondition.put(tables, conjunct);
				}
			}
			// Create predicates from merged expressions
			for (Expression condition : tablesToCondition.values()) {
				ExpressionInfo pred = new ExpressionInfo(this, condition);
				wherePredicates.add(pred);
			}
			// Separate into unary and join predicates
			for (ExpressionInfo exprInfo : wherePredicates) {
				if (exprInfo.aliasesMentioned.size() == 1) {
					unaryPredicates.add(exprInfo);
				} else {
					// Join predicate - calculate mentioned alias indexes
					Set<Integer> aliasIdxs = new HashSet<Integer>();
					for (String alias : exprInfo.aliasesMentioned) {
						aliasIdxs.add(aliasToIndex.get(alias));
					}
					joinedIndices.add(aliasIdxs);
					// Separate into equi- and non-equi join predicates
					Expression nonEquiPred = null;
					for (Expression conjunct : exprInfo.conjuncts) {
						ExpressionInfo curInfo = new ExpressionInfo(this, conjunct);
						Set<ColumnRef> curEquiJoinCols = extractEquiJoinCols(curInfo);
						if (curEquiJoinCols != null) {
							equiJoinCols.addAll(curEquiJoinCols);
							indexCols.addAll(curEquiJoinCols);
							equiJoinPreds.add(curInfo);
						} else {
							nonEquiPred = nonEquiPred==null?conjunct:
									new AndExpression(nonEquiPred, conjunct);
						}
					} // over conjuncts of join predicates
					// Add non-equi join predicates if any
					if (nonEquiPred != null) {
						ExpressionInfo curInfo = new ExpressionInfo(this, nonEquiPred);
						for (String alias: curInfo.aliasesMentioned) {
							int index = aliasToIndex.get(alias);
							TableInfo tableInfo = CatalogManager.currentDB.
									nameToTable.get(aliasToTable.get(alias));
							if (!(curInfo.finalExpression instanceof GreaterThanEquals ||
									curInfo.finalExpression instanceof GreaterThan ||
									curInfo.finalExpression instanceof MinorThanEquals ||
									curInfo.finalExpression instanceof MinorThan)) {
								if (tableInfo.tempTable || temporaryAlias.contains(alias)) {
									temporaryTables.add(index);
									for (String another: curInfo.aliasesMentioned) {
										if (!another.equals(alias)) {
											temporaryConnection.put(index, aliasToIndex.get(another));
										}
									}
								}
							}
						}
						nonEquiJoinPreds.add(curInfo);
						indexCols.addAll(extractNonEquiJoinCols(curInfo));
					}
				} // if join predicate
			} // over where conjuncts
//			joinConnection.forEach((leftIndex, set) -> {
//				if (set.size() > 1) {
//					for (Integer rightIndex: set) {
//						if (rightIndex > leftIndex && joinConnection.get(rightIndex).size() > 1) {
//							constraints.add(new ImmutablePair<>(leftIndex, rightIndex));
//						}
//					}
//				}
//			});
			joinConnection.forEach((leftIndex, set) -> {
				for (Integer rightIndex: set) {
					if (rightIndex > leftIndex) {
						constraints.add(new ImmutablePair<>(leftIndex, rightIndex));
					}
				}
			});
		} // if where clause
	}


	/**
	 * Convert each nonequi-predicates into a tree.
	 *
	 * @param context	context after preprocessing
	 */
	public void convertNonEquiPredicates(Context context) {
		// Build non-equi nodes
		NonEquiNodesTest nonEquiNodesTest = new NonEquiNodesTest(this, context.columnMapping);
		nonEquiJoinPreds.forEach(pred -> {
			pred.conjuncts.get(0).accept(nonEquiNodesTest);
			if (nonEquiNodesTest.nonEquiNodes.size() > 0)
				nonEquiJoinNodes.add(nonEquiNodesTest.nonEquiNodes.pop());
		});
	}
	/**
	 * Generate a mapping from table pairs to selectivity
	 *
	 * @param context	context after preprocessing
	 */
	public void extractSelectivity(Context context) {
		equiJoinPreds.forEach(expressionInfo -> {
			Iterator<Integer> tableIter = expressionInfo.aliasIdxMentioned.iterator();
			int priorTable = tableIter.next();
			int table = tableIter.next();
			int key = priorTable > table ? table * nrJoined + priorTable : priorTable * nrJoined + table;

			Index priorIndex = expressionInfo.indexMentioned.get(priorTable);
			Index curIndex = expressionInfo.indexMentioned.get(table);
			if (priorIndex instanceof IntPartitionIndex) {
				int priorKeys = ((IntPartitionIndex) priorIndex).nrKeys;
				int curKeys = ((IntPartitionIndex) curIndex).nrKeys;
				double selectivity = 1.0 / Math.max(priorKeys, curKeys);
				joinedSelectivity.put(key, selectivity);
			}
			else {
				int priorKeys = ((DoublePartitionIndex) priorIndex).nrKeys;
				int curKeys = ((DoublePartitionIndex) curIndex).nrKeys;
				double selectivity = 1.0 / Math.max(priorKeys, curKeys);
				joinedSelectivity.put(key, selectivity);
			}
		});
		nonEquiJoinPreds.forEach(expressionInfo -> {
			if (expressionInfo.aliasIdxMentioned.size() == 2) {
				Iterator<Integer> tableIter = expressionInfo.aliasIdxMentioned.iterator();
				int priorTable = tableIter.next();
				int table = tableIter.next();

				Iterator<String> aliasIter = expressionInfo.aliasesMentioned.iterator();
				String priorAlias = aliasIter.next();
				String curAlias = aliasIter.next();

				int key = priorTable > table ? table * nrJoined + priorTable : priorTable * nrJoined + table;

				int priorKeys = CatalogManager.getCardinality(context.aliasToFiltered.get(priorAlias));
				int curKeys = CatalogManager.getCardinality(context.aliasToFiltered.get(curAlias));

				double selectivity = 1.0 / Math.max(priorKeys, curKeys);
				double base = joinedSelectivity.getOrDefault(key, 1.0);
				joinedSelectivity.put(key, selectivity * base);
			}
		});
	}

	/**
	 * Adds expressions in the GROUP-By clause (if any).
	 */
	void treatGroupBy() throws Exception {		
		if (plainSelect.getGroupByColumnReferences() != null) {
			for (Expression groupExpr : 
				plainSelect.getGroupByColumnReferences()) {
				groupByExpressions.add(new ExpressionInfo(this, groupExpr));
			}
			// Verify that select clause and group-by clause
			// are consistent (each entry in the select clause
			// must be either an aggregate or an expression that
			// appears in the group-by clause).
			Set<String> groupByStrings = new HashSet<>();
			for (ExpressionInfo groupExpr : groupByExpressions) {
				groupByStrings.add(groupExpr.finalExpression.toString());
			}
			for (ExpressionInfo selectExpr : selectExpressions) {
				if (!selectExpr.resultScope.equals(ExpressionScope.PER_GROUP) &&
						!groupByStrings.contains(selectExpr.finalExpression.toString())) {
					throw new SQLexception("Error - select item " + selectExpr + 
							" is neither aggregate nor does it appear in group by " +
							"clause");
				}
			}
		} // if group-by clause present
	}
	/**
	 * Adds expression in HAVING clause.
	 */
	void treatHaving() throws Exception {
		Expression having = plainSelect.getHaving();
		if (having != null) {
			havingExpression = new ExpressionInfo(this, having);
		} else {
			havingExpression = null;
		}
	}
	/**
	 * Adds expression in ORDER BY clause.
	 */
	void treatOrderBy() throws Exception {
		List<OrderByElement> orderElements = plainSelect.getOrderByElements();
		if (orderElements != null) {
			int nrOrderElements = orderElements.size();
			orderByAsc = new boolean[nrOrderElements];
			for (int orderCtr=0; orderCtr<nrOrderElements; ++orderCtr) {
				OrderByElement orderElement = orderElements.get(orderCtr);
				Expression expr = orderElement.getExpression();
				ExpressionInfo exprInfo = new ExpressionInfo(this, expr);
				orderByExpressions.add(exprInfo);
				boolean isAscending = orderElement.isAsc();
				orderByAsc[orderCtr] = isAscending;
			}
		}
	}
	/**
	 * Collect aggregates from SELECT and HAVING clause.
	 */
	void collectAggregates() throws Exception {
		// Collect expressions that may contain aggregates
		List<ExpressionInfo> exprsWithAggs = new ArrayList<>();
		exprsWithAggs.addAll(selectExpressions);
		if (havingExpression!=null) {
			exprsWithAggs.add(havingExpression);			
		}
		// Collect aggregates with additional information
		for (ExpressionInfo exprWithAgg: exprsWithAggs) {
			for (Function agg : exprWithAgg.aggregates) {
				aggregates.add(new AggInfo(this, agg));
			}
		}
	}
	/**
	 * Collects columns required for steps after pre-processing.
	 */
	void collectRequiredCols() {
		colsForJoins.addAll(equiJoinCols);
		colsForJoins.addAll(extractCols(nonEquiJoinPreds));
		colsForPostProcessing.addAll(extractCols(selectExpressions));
		colsForPostProcessing.addAll(extractCols(groupByExpressions));
		if (havingExpression != null) {
			colsForPostProcessing.addAll(havingExpression.columnsMentioned);			
		}
		colsForPostProcessing.addAll(extractCols(orderByExpressions));
		// Add dummy column if no columns are selected for post-processing
		// (otherwise certain queries would not be treated correctly,
		// e.g. if select clause contains constant expressions).
		if (colsForPostProcessing.isEmpty()) {
			// (assumes from clause)
			String alias = aliases[0];
			String table = aliasToTable.get(alias);
			TableInfo tableInfo = CatalogManager.
					currentDB.nameToTable.get(table);
			// If at least one table in the from clause
			// has no columns then the join result is
			// empty so no dummy columns are required.
			if (!tableInfo.columnNames.isEmpty()) {
				String column = tableInfo.columnNames.get(0);
				ColumnRef colRef = new ColumnRef(alias, column);
				colsForPostProcessing.add(colRef);
			}
		}
	}
	/**
	 * Extracts a list of all columns mentioned in a list of
	 * expressions.
	 * 
	 * @param expressions	list of expressions to extract columns from
	 * @return				set of references to mentioned columns
	 */
	static Set<ColumnRef> extractCols(List<ExpressionInfo> expressions) {
		Set<ColumnRef> colRefs = new HashSet<ColumnRef>();
		for (ExpressionInfo expr : expressions) {
			colRefs.addAll(expr.columnsMentioned);
		}
		return colRefs;
	}
	/**
	 * Determines query aggregation type.
	 * 
	 * @return	aggregation type of this query
	 */
	AggregationType getAggregationType() {
		if (aggregates.isEmpty()) {
			// No aggregates means no aggregation
			return AggregationType.NONE;
		} else if (groupByExpressions.isEmpty()) {
			// Aggregates but no group by
			return AggregationType.ALL_ROWS;
		} else {
			// Aggregates and group by
			return AggregationType.GROUPS;
		}
	}
	/**
	 * Returns true if there it at least one join predicate
	 * connecting the set of items in the FROM clause to the
	 * single item. We assume that the new table is not in
	 * the set of already joined tables.
	 * 
	 * @param aliasIndices	indexes of aliases already joined
	 * @param newIndex		index of new alias to check
	 * @return				true iff join predicates connect
	 */
	public boolean connected(Set<Integer> aliasIndices, int newIndex) {
		// Resulting join indices if selecting new table for join
		Set<Integer> indicesAfterJoin = new HashSet<Integer>();
		indicesAfterJoin.addAll(aliasIndices);
		indicesAfterJoin.add(newIndex);
		// Is there at least one connecting join predicate?
		for (Set<Integer> joined : joinedIndices) {
			if (indicesAfterJoin.containsAll(joined) &&
					joined.contains(newIndex)) {
				return true;
			}
		}
		return false;
	}
	/**
	 * Returns the selectivity estimation.
	 *
	 * @param joinedTables	tables that have been joined.
	 * @param table			new table to join
	 * @return				estimated selectivity
	 */
	public double estimate(Set<Integer> joinedTables, int table) {
		double selectivity = 1;
		for (Integer priorTable: joinedTables) {
			int key = priorTable > table ? table * nrJoined + priorTable : priorTable * nrJoined + table;
			selectivity *= joinedSelectivity.getOrDefault(key, 1.0);
		}
		return selectivity;
	}
	/**
	 * Concatenates string representations of given expression
	 * list, using the given separator.
	 * 
	 * @param expressions	list of expressions to concatenate
	 * @param separator		separator to insert between elements
	 * @return				concatenation string
	 */
	String concatenateExprs(List<ExpressionInfo> expressions, String separator) {
		List<String> toConcat = new ArrayList<String>();
		for (ExpressionInfo expr : expressions) {
			toConcat.add(expr.toString());
		}
		return StringUtils.join(toConcat, separator);
	}
	/**
	 * Prints out log entry about query analysis
	 * if corresponding logging flag is set.
	 * 
	 * @param logEntry	text to display if logging is activated
	 */
	static void log(String logEntry) {
		if (LoggingConfig.QUERY_ANALYSIS_VERBOSE) {
			System.out.println(logEntry);
		}
	}
	/**
	 * Extracts the limit on the number of result tuples
	 * and returns -1 if none specified.
	 * 
	 * @param plainSelect	input query
	 * @return				result tuple limit or -1 if none specified
	 * @throws Exception
	 */
	static int getLimit(PlainSelect plainSelect) throws Exception {
		Limit limitObj = plainSelect.getLimit();
		// No limit specified?
		if (limitObj == null) {
			return -1;
		}
		Expression limitExpr = limitObj.getRowCount();
		// No row limit specified?
		if (limitExpr == null) {
			return -1;
		}
		// Only constant row limits are supported
		if (!(limitExpr instanceof LongValue)) {
			throw new SQLexception("Error - only constant limits supported");
		} else {
			return (int)(((LongValue)limitExpr).getValue());
		}
	}

	/**
	 * Generate unique id for each predicate.
	 */
	void maintainPredicatesID() {
		unaryPredicates.forEach(predicate -> {
			String pstr = predicate.toString();
			int pid = BufferManager.predicateToID.getOrDefault(pstr, -1);
			if (pid < 0) {
				pid = BufferManager.predicateToID.size();
				BufferManager.predicateToID.put(pstr, pid);
			}
			predicate.pid = pid;
		});
		int id = 0;
		for (ExpressionInfo predicate: equiJoinPreds) {
			predicate.pid = id;
			id++;
		}
	}

	/**
	 * Analyzes a select query to prepare processing.
	 * 
	 * @param plainSelect	a plain select query
	 * @param explain		whether this is an explain query
	 * @param plotAtMost	plot at most that many plots (in explain mode)
	 * @param plotEvery		generate one plot after that many samples (in explain mode)
	 * @param plotDir		add plots to this directory (in explain mode)
	 */
	public QueryInfo(PlainSelect plainSelect, boolean explain,
			int plotAtMost, int plotEvery, String plotDir) throws Exception {
		log("Input query: " + plainSelect);
		this.plainSelect = plainSelect;
		this.explain = explain;
		this.plotAtMost = plotAtMost;
		this.plotEvery = plotEvery;
		this.plotDir = plotDir;
		// Extract information in FROM clause
		extractFromInfo();
		log("Alias -> table: " + aliasToTable);
		log("Column info: " + colRefToInfo);
		// Add implicit references to aliases
		addImplicitRefs();
		log("Unique column name -> alias: " + columnToAlias);
		// Resolve wildcards and add aliases for SELECT clause
		treatSelectClause();
		log("Select expressions: " + selectExpressions);
		log("Select aliases: " + selectToAlias);
		log("Alias to expression: " + aliasToExpression);
		// Extract predicates in WHERE clause
		extractPredicates();
		log("Unary predicates: " + unaryPredicates);
		log("Equi join cols: " + equiJoinCols);
		log("Equi join preds: " + equiJoinPreds);
		log("Other join preds: " + nonEquiJoinPreds);
		// Assign integers to predicates
		maintainPredicatesID();
		// Add expressions in GROUP BY clause
		treatGroupBy();
		log("GROUP BY expressions: " + groupByExpressions);
		// Add expression in HAVING clause
		treatHaving();
		log("HAVING clause: " + (havingExpression!=null?havingExpression:"none"));
		// Adds expressions in ORDER BY clause
		treatOrderBy();
		log("ORDER BY expressions: " + orderByExpressions);
		// Collect required columns
		collectRequiredCols();
		log("Required cols for joins: " + colsForJoins);
		log("Required for post-processing: " + colsForPostProcessing);
		// Collect aggregates
		collectAggregates();
		log("Extracted aggregates: " + aggregates);
		// Set aggregation type
		aggregationType = getAggregationType();
		log("Aggregation type:\t" + aggregationType);
		// Set result tuple limit
		limit = getLimit(plainSelect);
		log("Limit:\t" + limit);
	}

	/**
	 * Analyzes a select query to prepare processing.
	 *
	 * @param plainSelect	a plain select query
	 * @param explain		whether this is an explain query
	 * @param plotAtMost	plot at most that many plots (in explain mode)
	 * @param plotEvery		generate one plot after that many samples (in explain mode)
	 * @param plotDir		add plots to this directory (in explain mode)
	 */
	public QueryInfo(PlainSelect plainSelect, Set<String> temporary, boolean explain,
					 int plotAtMost, int plotEvery, String plotDir) throws Exception {
		log("Input query: " + plainSelect);
		this.plainSelect = plainSelect;
		this.explain = explain;
		this.plotAtMost = plotAtMost;
		this.plotEvery = plotEvery;
		this.plotDir = plotDir;
		this.temporaryAlias.addAll(temporary);
		// Extract information in FROM clause
		extractFromInfo();
		log("Alias -> table: " + aliasToTable);
		log("Column info: " + colRefToInfo);
		// Add implicit references to aliases
		addImplicitRefs();
		log("Unique column name -> alias: " + columnToAlias);
		// Resolve wildcards and add aliases for SELECT clause
		treatSelectClause();
		log("Select expressions: " + selectExpressions);
		log("Select aliases: " + selectToAlias);
		log("Alias to expression: " + aliasToExpression);
		// Extract predicates in WHERE clause
		extractPredicates();
		log("Unary predicates: " + unaryPredicates);
		log("Equi join cols: " + equiJoinCols);
		log("Equi join preds: " + equiJoinPreds);
		log("Other join preds: " + nonEquiJoinPreds);
		// Assign integers to predicates
		maintainPredicatesID();
		// Add expressions in GROUP BY clause
		treatGroupBy();
		log("GROUP BY expressions: " + groupByExpressions);
		// Add expression in HAVING clause
		treatHaving();
		log("HAVING clause: " + (havingExpression!=null?havingExpression:"none"));
		// Adds expressions in ORDER BY clause
		treatOrderBy();
		log("ORDER BY expressions: " + orderByExpressions);
		// Collect required columns
		collectRequiredCols();
		log("Required cols for joins: " + colsForJoins);
		log("Required for post-processing: " + colsForPostProcessing);
		// Collect aggregates
		collectAggregates();
		log("Extracted aggregates: " + aggregates);
		// Set aggregation type
		aggregationType = getAggregationType();
		log("Aggregation type:\t" + aggregationType);
		// Set result tuple limit
		limit = getLimit(plainSelect);
		log("Limit:\t" + limit);
	}
}