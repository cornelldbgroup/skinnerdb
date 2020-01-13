package unnesting;

import java.util.*;

import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import catalog.info.TableInfo;
import config.NamingConfig;
import expressions.ExpressionInfo;
import expressions.normalization.CollectReferencesVisitor;
import expressions.normalization.CopyVisitor;
import expressions.normalization.SubstitutionVisitor;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
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
import query.where.WhereUtil;

/**
 * Decomposes a query that may contain nested (potentially
 * correlated) sub-queries into a sequence of simple queries.
 *
 * @author immanueltrummer
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
     * Contains column references that became available in
     * certain sub-queries, the current sub-query is on top.
     */
    public Stack<Set<ColumnRef>> scopeCols = new Stack<>();
    /**
     * Contains unary predicates that became available in
     * certain sub-queries, the current sub-query is on top.
     */
    public Stack<Set<Expression>> unaryPredicates = new Stack<>();
    /**
     * Contains a mapping from a predicate to
     * the alias that contains the predicate.
     */
    public Map<String, String> predsScopes = new HashMap<>();
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
     * Contains on top tables containing sub-query results
     * that need to be added to FROM clause of current query.
     */
    public Stack<List<Table>> addToFrom = new Stack<>();
    /**
     * List of correlated predicates that need to be added to
     * WHERE clause (since they cannot be resolved in inner
     * scope).
     */
    public Stack<Expression> addToOuterWhere = new Stack<>();
    /**
     * List of temporary tables that are
     * extracted from the inner sub queries.
     */
    public List<Set<String>> temporaryTables = new ArrayList<>();

    /**
     * Updates current scope and alias-to-columns mapping
     * based on base tables that appear in FROM clause of
     * given query.
     *
     * @param plainSelect extract columns for this query's FROM clause
     */
    void treatSimpleFrom(PlainSelect plainSelect) {
        // Retrieve fields to update
        Set<ColumnRef> curScopeCols = scopeCols.peek();
        Map<String, List<String>> curAliasToCols = aliasToCols.peek();
        // Get all items in FROM clause
        List<FromItem> fromItems = FromUtil.allFromItems(plainSelect);
        // Iterate over base tables in FROM clause
        for (FromItem fromItem : fromItems) {
            if (fromItem instanceof Table) {
                // Extract table and alias name (defaults to table name)
                Table table = (Table) fromItem;
                String tableName = table.getName().toLowerCase();
                String alias = table.getAlias() != null ?
                        table.getAlias().getName() : tableName;
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
                    predsScopes.putIfAbsent(colName, alias);
                }
            }
        }
    }

    /**
     * Unnests one single from item and returns unnested version.
     * Also updates the current column scope by adding references
     * to sub-query result columns and alias-to-column mapping.
     *
     * @param fromItem    original from item to unnest
     * @param scopeCols   available columns in current scope
     * @param aliasToCols maps aliases to associated columns
     * @return updated from item after unnesting
     */
    FromItem unnestFromItem(FromItem fromItem, Set<ColumnRef> scopeCols,
                            Map<String, List<String>> aliasToCols) {
        // Does from item need unnesting?
        if (fromItem instanceof SubSelect) {
            SubSelect subSelect = (SubSelect) fromItem;
            String alias = subSelect.getAlias().getName().toLowerCase();
            SelectBody selectBody = subSelect.getSelectBody();
            if (selectBody instanceof PlainSelect) {
                // Sub-query will be replaced by table reference
                Table table = new Table(alias);
                // Associate unnested sub-query with name
                PlainSelect plainSelect = (PlainSelect) selectBody;
                plainSelect.setIntoTables(Arrays.asList(
                        new Table[]{table}));
                // Recursively unnest sub-query
                selectBody.accept(this);
                // Update scope and column mapping
                List<String> newCols = subqueryFields.pop();
                for (String col : newCols) {
                    scopeCols.add(new ColumnRef("", col));
                    scopeCols.add(new ColumnRef(alias, col));
                }
                aliasToCols.put(alias, newCols);
                // Return table that will contain sub-query result
                return (FromItem) table;
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
     * @param plainSelect query whose FROM clause is unnested
     */
    void treatNestedFrom(PlainSelect plainSelect) {
        // Retrieve current scope
        Set<ColumnRef> curScopeCols = scopeCols.peek();
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
     * @param plainSelect replace wildcards in this query's select clause
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
                AllTableColumns allTblCols = (AllTableColumns) curItem;
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
     * @param plainSelect query for which to register result
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
            subqueryFields.push(selectNames);
        } catch (SQLexception e) {
            sqlExceptions.add(e);
        }
    }

    /**
     * Expand FROM clause by adding tables containing
     * results of nested queries that were rewritten
     * during unnesting.
     *
     * @param plainSelect expand this query's FROM clause
     */
    void expandFrom(PlainSelect plainSelect) {
        List<Table> addToThisFrom = addToFrom.peek();
        for (Table toAdd : addToThisFrom) {
            expandTable(plainSelect, toAdd);
        }
    }

    void expandTable(PlainSelect plainSelect, Table toAdd) {
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

    /**
     * Separate predicates that refer to columns in outer
     * (as opposed to current) query scope.
     *
     * @param plainSelect analyze WHERE clause of this query
     */
    void separateNonLocalPreds(PlainSelect plainSelect) {
        // Extract conjuncts in original WHERE clause
        Expression where = plainSelect.getWhere();
        if (where == null) {
            // Nothing to do
            return;
        }
        List<Expression> conjuncts = new ArrayList<>();
        WhereUtil.extractConjuncts(where, conjuncts);
        // Separate local and non-local predicates
        Set<ColumnRef> curScope = scopeCols.pop();
        List<Expression> localConjuncts = new ArrayList<>();
        for (Expression conjunct : conjuncts) {
            CollectReferencesVisitor collector = new CollectReferencesVisitor();
            conjunct.accept(collector);
            boolean equal = false;
            if (conjunct instanceof EqualsTo) {
                EqualsTo equalsTo = (EqualsTo) conjunct;
                equal = equalsTo.getLeftExpression() instanceof Column
                        && equalsTo.getRightExpression() instanceof Column;
            } else if (conjunct instanceof NotEqualsTo) {
                NotEqualsTo notEqualsTo = (NotEqualsTo) conjunct;
                equal = notEqualsTo.getLeftExpression() instanceof Column
                        && notEqualsTo.getRightExpression() instanceof Column;
            }
            // Is it a local predicate?
            if (curScope.containsAll(collector.mentionedColumns)) {
                localConjuncts.add(conjunct);
            } else {
                // Raise exceptions if there is no outer scope
                // (which may allow to resolve unknown references).
                if (scopeCols.isEmpty()) {
                    sqlExceptions.add(new SQLexception("Error - "
                            + "predicate " + conjunct + " contains "
                            + "unresolved references but no outer "
                            + "scope is specified. Current scope: "
                            + curScope.toString()));
                }
                Set<ColumnRef> outerScope = scopeCols.peek();
                // Raise exception if predicate is no binary equality
                // between two query columns.
                if (conjunct instanceof EqualsTo) {
                    EqualsTo equalsTo = (EqualsTo) conjunct;
                    if (!(equalsTo.getLeftExpression() instanceof Column) ||
                            !(equalsTo.getRightExpression() instanceof Column) ||
                            equalsTo.isNot()) {
                        sqlExceptions.add(new SQLexception("Error - "
                                + "sub-queries may only be correlated "
                                + "via binary equality predicates with "
                                + "column references as operands ("
                                + conjunct + ")"));
                    }
                } else if (conjunct instanceof NotEqualsTo) {
                    NotEqualsTo notEqualsTo = (NotEqualsTo) conjunct;
                    if (!(notEqualsTo.getLeftExpression() instanceof Column) ||
                            !(notEqualsTo.getRightExpression() instanceof Column) ||
                            notEqualsTo.isNot()) {
                        sqlExceptions.add(new SQLexception("Error - "
                                + "sub-queries may only be correlated "
                                + "via binary equality predicates with "
                                + "column references as operands ("
                                + conjunct + ")"));
                    }
                } else {
                    sqlExceptions.add(new SQLexception("Error - "
                            + "sub-queries may only be correlated "
                            + "via binary equality predicates ("
                            + conjunct + ")"));
                }
                // Obtain table that will contain sub-query result
                Table resultTbl = plainSelect.getIntoTables().get(0);
                // Need to make sure that local
                // references are still available
                // in outer query scope.
                List<SelectItem> selects = plainSelect.getSelectItems();
                try {
                    // Determine whether query contains aggregates
                    boolean isAggregation = SelectUtil.hasAggregates(selects);
                    // Iterate over columns mentioned in current predicate
                    for (ColumnRef oldColRef : collector.mentionedColumns) {
                        if (curScope.contains(oldColRef)) {
                            // Generate new unique column alias
                            String newColName = NamingConfig.SUBQUERY_COL_PRE
                                    + nextAttributeID;
                            ++nextAttributeID;
                            // Obtain string representation of old column
                            String aliasName = oldColRef.aliasName;
                            Table table = aliasName.isEmpty() ?
                                    null : new Table(aliasName);
                            String colName = oldColRef.columnName;
                            Column oldCol = new Column(table, colName);
                            String oldColString = oldCol.toString().toLowerCase();
                            // Create substitution map
                            Map<String, Expression> substitutionMap =
                                    new HashMap<>();
                            Column newCol = new Column(resultTbl, newColName);
                            substitutionMap.put(oldColString, newCol);
                            // Substitute column references
                            SubstitutionVisitor substitutor =
                                    new SubstitutionVisitor(substitutionMap);
                            conjunct.accept(substitutor);
                            conjunct = substitutor.exprStack.pop();
                            // Add new column to select items
                            SelectExpressionItem newItem =
                                    new SelectExpressionItem(oldCol);
                            Alias newColAlias = new Alias(newColName);
                            newItem.setAlias(newColAlias);
                            selects.add(newItem);
                            // Add new column to group by clause for aggregates
                            if (isAggregation && equal) {
                                plainSelect.addGroupByColumnReference(oldCol);
                            }
                            // Add new column to outer query scope
                            ColumnRef newColRef = new ColumnRef("", newColName);
                            outerScope.add(newColRef);
                        }
                    }
                } catch (SQLexception e) {
                    sqlExceptions.add(e);
                }
                // This condition will be added to
                // WHERE clause of outer query.
                addToOuterWhere.push(conjunct);
            } // whether local predicate
        } // over conjucts
        // Form WHERE clause from local predicates
        Expression localWhere = WhereUtil.conjunction(localConjuncts);
        plainSelect.setWhere(localWhere);
        // Put current scope back on stack
        scopeCols.push(curScope);
    }

    /**
     * Adds predicates to WHERE clause that were propagated up
     * from nested sub-queries as they refer to columns that
     * are part of the outer scope.
     *
     * @param plainSelect add predicates to this query's WHERE clause
     */
    void addNonLocalPreds(PlainSelect plainSelect) {
        Expression outerConjuncts = null;
        while (!addToOuterWhere.isEmpty()) {
            Expression conjunct = addToOuterWhere.pop();
            if (outerConjuncts == null) {
                outerConjuncts = conjunct;
            } else {
                outerConjuncts = new AndExpression(outerConjuncts, conjunct);
            }
        }
        if (outerConjuncts != null) {
            addPredicates(plainSelect, outerConjuncts);
        }
    }

    /**
     * Adds predicates to WHERE clause.
     *
     * @param plainSelect add predicates to this query's WHERE clause
     * @param predicates  predicates to query's WHERE clause
     */
    void addPredicates(PlainSelect plainSelect, Expression predicates) {
        Expression curWhere = plainSelect.getWhere();
        if (curWhere == null) {
            plainSelect.setWhere(predicates);
        } else {
            plainSelect.setWhere(new AndExpression(curWhere, predicates));
        }
    }

    /**
     * Adds predicates to WHERE clause that were propagated up
     * from nested sub-queries as they are unary.
     *
     * @param plainSelect add predicates to this query's WHERE clause
     */
    void addUnaryPreds(PlainSelect plainSelect) {
        Set<Expression> unary = unaryPredicates.peek();
        Map<String, List<String>> curAliasToCols = aliasToCols.peek();
        Set<ColumnRef> curScopeCols = scopeCols.peek();
        Iterator<Expression> iterator = unary.iterator();
        while (iterator.hasNext()) {
            Expression unaryPredicate = iterator.next();
            Expression curWhere = plainSelect.getWhere();
            if (curWhere == null) {
                plainSelect.setWhere(unaryPredicate);
            } else {
                plainSelect.setWhere(new AndExpression(curWhere, unaryPredicate));
            }

            String tableName = "";
            boolean added = false;
            if (unaryPredicate instanceof EqualsTo) {
                EqualsTo equalsTo = (EqualsTo) unaryPredicate;
                Expression left = equalsTo.getLeftExpression();
                Expression right = equalsTo.getRightExpression();
                if (left instanceof Column) {
                    tableName = predsScopes.getOrDefault(((Column) left).getColumnName(), "");
                } else {
                    tableName = predsScopes.getOrDefault(((Column) right).getColumnName(), "");
                }
            }
            Table toAdd = new Table(tableName);
            FromItem curFrom = plainSelect.getFromItem();
            if (curFrom == null) {
                plainSelect.setFromItem(toAdd);
                added = true;
            } else {
                // Make sure that join list is initialized
                String fromName = ((Table) curFrom).getName();
                if (!fromName.equals(tableName)) {
                    List<Join> joins = plainSelect.getJoins();
                    if (joins == null) {
                        joins = new ArrayList<>();
                        plainSelect.setJoins(joins);
                        added = true;
                    } else {
                        if (joins.stream().filter(j ->
                                ((Table) j.getRightItem()).getName().equals(toAdd.getName()))
                                .findFirst().orElse(null) == null) {
                            // Add new table via simple join
                            Join join = new Join();
                            join.setSimple(true);
                            join.setRightItem(toAdd);
                            joins.add(join);
                            added = true;
                        }
                    }
                }
            }
            if (added) {
                String alias = toAdd.getAlias() != null ?
                        toAdd.getAlias().getName() : tableName;
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
                    predsScopes.putIfAbsent(colName, alias);
                }
            }

            iterator.remove();
        }
    }

    /**
     * Unnests WHERE clause of input query.
     *
     * @param plainSelect rewrite WHERE clause of this query
     */
    void treatWhere(PlainSelect plainSelect) {
        Expression originalWhere = plainSelect.getWhere();
        if (originalWhere != null) {
            // Decompose where clause into conjuncts
            List<Expression> conjuncts = new ArrayList<>();
            WhereUtil.extractConjuncts(originalWhere, conjuncts);
            // Rewrite each conjunct separately
            List<Expression> unnestedConjuncts = new ArrayList<>();
            // TODO: check whether it is unary predicate
            Set<Expression> subUnary = unaryPredicates.peek();
            for (Expression conjunct : conjuncts) {
                if (conjunct instanceof EqualsTo) {
                    EqualsTo equalsTo = (EqualsTo) conjunct;
                    if (!(equalsTo.getLeftExpression() instanceof Column
                            && equalsTo.getRightExpression() instanceof Column)) {
                        subUnary.add(conjunct);
                    }
                }
            }
            for (Expression conjunct : conjuncts) {
                // Treat special case: expression in sub-query result
                if (conjunct instanceof InExpression) {
                    InExpression inExpr = (InExpression) conjunct;
                    Expression left = inExpr.getLeftExpression();
                    ItemsList right = inExpr.getRightItemsList();
                    if (left != null && right instanceof SubSelect) {
                        left.accept(this);
                        Expression unnestedLeft = exprStack.pop();
                        SubSelect rightSubSel = (SubSelect) right;
                        rightSubSel.accept(this);
                        if (!inExpr.isNot()) {
                            Expression unnestedRight = exprStack.pop();
                            EqualsTo unnestedConjunct = new EqualsTo();
                            unnestedConjunct.setNot();
                            unnestedConjunct.setLeftExpression(unnestedLeft);
                            unnestedConjunct.setRightExpression(unnestedRight);
                            unnestedConjuncts.add(unnestedConjunct);
                        }
                        // TODO: handle not all in a table
                        else {
                            Expression unnestedRight = exprStack.pop();
                            NotEqualsTo unnestedConjunct = new NotEqualsTo();
                            unnestedConjunct.setLeftExpression(unnestedLeft);
                            unnestedConjunct.setRightExpression(unnestedRight);
                            unnestedConjunct.setNot();
                            unnestedConjuncts.add(unnestedConjunct);
                        }
                        continue;
                    }
                }
                else if (conjunct instanceof ExistsExpression) {
                    ExistsExpression existExpr = (ExistsExpression) conjunct;
                    if (existExpr.isNot()) {
                        PlainSelect subSelect = (PlainSelect) ((SubSelect) existExpr
                                .getRightExpression()).getSelectBody();
//                        List<SelectItem> selectItems = new ArrayList<>();
//                        Function function = new Function();
//                        function.setName("COUNT");
//                        function.setAllColumns(true);
//                        selectItems.add(new SelectExpressionItem(function));
//                        subSelect.setSelectItems(selectItems);
//                        int nrOuters = addToOuterWhere.size();
//                        // Name anonymous sub-query
//                        String alias = NamingConfig.SUBQUERY_PRE + nextSubqueryID;
//                        ++nextSubqueryID;
//                        // Unnest nested sub-query if possible
//                        // Rewrite sub-query and add to query list
//                        Table resultTable = new Table(alias);
//                        subSelect.setIntoTables(Arrays.asList(
//                                new Table[]{resultTable}));
//                        subSelect.accept(this);
//                        // Replace nested sub-query by table reference
//                        List<String> subqueryCols = subqueryFields.pop();
//                        String firstCol = subqueryCols.get(0);
//                        exprStack.push(new Column(resultTable, firstCol));
//                        // Add sub-query fields to scope
//                        Set<ColumnRef> curScope = scopeCols.peek();
//                        for (String subQueryCol : subqueryCols) {
//                            // Only allow fully qualified references -
//                            // the only references should be generated
//                            // during unnesting anyway.
//                            //curScope.add(new ColumnRef("", subQueryCol));
//                            curScope.add(new ColumnRef(alias, subQueryCol));
//                        }
//                        // Schedule table containing sub-query result to
//                        // be added to FROM clause.
//                        addToFrom.peek().add(resultTable);
//                        int newOuters = addToOuterWhere.size();
//                        Expression orExpr = null;
//                        while (addToOuterWhere.size() != nrOuters) {
//                            Expression notExistOuter = addToOuterWhere.pop();
//                            if (orExpr == null) {
//                                orExpr = notExistOuter;
//                            }
//                            else {
//                                orExpr = new AndExpression(orExpr, notExistOuter);
//                            }
//                        }
//                        if (orExpr != null) {
//                            orExpr = new NotExpression(orExpr);
//                        }
//                        addToOuterWhere.add(orExpr);
                        // expand tables
                        List<FromItem> fromItems = FromUtil.allFromItems(subSelect);
                        List<Table> outerTables = addToFrom.peek();
                        Set<String> temporary = temporaryTables.get(addToFrom.size() - 1);
                        treatSimpleFrom(subSelect);
                        for (FromItem fromItem : fromItems) {
                            if (fromItem instanceof Table) {
                                // Extract table and alias name (defaults to table name)
                                Table table = (Table) fromItem;
                                String alias = table.getAlias()!=null?
                                        table.getAlias().getName().toLowerCase():
                                        table.getName().toLowerCase();
                                outerTables.add(table);
                                temporary.add(alias);
                            }
                        }
                        // extract predicates
                        Expression wheres = subSelect.getWhere();
                        if (wheres instanceof AndExpression) {
                            AndExpression andWheres = (AndExpression) wheres;
                            NotExpression notExpression = new NotExpression(andWheres);
                            unnestedConjuncts.add(notExpression);
                        }
                    }
                    else {
                        PlainSelect subSelect = (PlainSelect) ((SubSelect)existExpr.getRightExpression()).getSelectBody();
//                        List<SelectItem> selectItems = new ArrayList<>();
//                        Function function = new Function();
//                        function.setName("COUNT");
//                        function.setAllColumns(true);
//                        selectItems.add(new SelectExpressionItem(function));
//                        subSelect.setSelectItems(selectItems);
//                        GreaterThan greaterThan = new GreaterThan();
//                        greaterThan.setLeftExpression(existExpr.getRightExpression());
//                        greaterThan.setRightExpression(new LongValue(0));
//                        greaterThan.accept(this);

                        // expand tables
                        List<FromItem> fromItems = FromUtil.allFromItems(subSelect);
                        List<Table> outerTables = addToFrom.peek();
                        Set<String> temporary = temporaryTables.get(addToFrom.size() - 1);
                        treatSimpleFrom(subSelect);
                        for (FromItem fromItem : fromItems) {
                            if (fromItem instanceof Table) {
                                // Extract table and alias name (defaults to table name)
                                Table table = (Table) fromItem;
                                String alias = table.getAlias()!=null?
                                        table.getAlias().getName().toLowerCase():
                                        table.getName().toLowerCase();
                                outerTables.add(table);
                                temporary.add(alias);
                            }
                        }
                        // extract predicates
                        Expression wheres = subSelect.getWhere();
                        if (wheres instanceof AndExpression) {
                            AndExpression andWheres = (AndExpression) wheres;
                            andWheres.setNot();
                            unnestedConjuncts.add(andWheres);
                        }
                    }
                    continue;
                }
                // Default unnesting strategy
                conjunct.accept(this);
                unnestedConjuncts.add(exprStack.pop());
            }
            // Replace where clause by unnested version
            plainSelect.setWhere(WhereUtil.conjunction(unnestedConjuncts));
        }
    }

    List<Column> extractColumnsInExist(ExistsExpression existExpr) {
        List<Column> columns = new ArrayList<>();
        PlainSelect subSelect = (PlainSelect) ((SubSelect) existExpr
                .getRightExpression()).getSelectBody();
        List<FromItem> fromItems = FromUtil.allFromItems(subSelect);
        // Iterate over base tables in FROM clause
        Set<String> inTables = new HashSet<>();
        Set<ColumnRef> curScopeCols = new HashSet<>();
        for (FromItem fromItem : fromItems) {
            if (fromItem instanceof Table) {
                // Extract table and alias name (defaults to table name)
                Table table = (Table) fromItem;
                String tableName = table.getName();
                String alias = table.getAlias() != null ?
                        table.getAlias().getName() : tableName;
                // Extract associated column references
                TableInfo tableInfo = CatalogManager.currentDB.
                        nameToTable.get(tableName);
                // Update scope and mappings
                for (ColumnInfo colInfo : tableInfo.nameToCol.values()) {
                    String colName = colInfo.name;
                    // Update current scope
                    curScopeCols.add(new ColumnRef("", colName));
                    curScopeCols.add(new ColumnRef(alias, colName));
                }
            }
        }
        List<Expression> subConjuncts = new ArrayList<>();
        WhereUtil.extractConjuncts(subSelect.getWhere(), subConjuncts);
        Column outerColumn = null;
        Column innerColumn = null;
        for (Expression sub : subConjuncts) {
            if (sub instanceof EqualsTo) {
                EqualsTo subEqual = (EqualsTo) sub;
                if (subEqual.getLeftExpression()
                        instanceof Column && subEqual.getRightExpression() instanceof Column) {
                    Column leftColumn = (Column) subEqual.getLeftExpression();
                    Column rightColumn = (Column) subEqual.getRightExpression();
                    Table leftTable = leftColumn.getTable();
                    Table rightTable = rightColumn.getTable();
                    String leftTableName = leftTable == null
                            ? "" :
                            (leftTable.getAlias() != null ? leftTable.getAlias().getName() :
                                    (leftTable.getName() == null ? "" : leftTable.getName()));
                    String rightTableName = rightTable == null
                            ? "" :
                            (rightTable.getAlias() != null ? rightTable.getAlias().getName() :
                                    (rightTable.getName() == null ? "" : rightTable.getName()));
                    ColumnRef leftRef = new ColumnRef(leftTableName, leftColumn.getColumnName());
                    ColumnRef rightRef = new ColumnRef(rightTableName, rightColumn.getColumnName());
                    if (!curScopeCols.contains(leftRef)) {
                        outerColumn = leftColumn;
                        innerColumn = rightColumn;
                    }
                    else if (!curScopeCols.contains(rightRef)){
                        outerColumn = rightColumn;
                        innerColumn = leftColumn;
                    }
                }
            }
        }
        columns.add(outerColumn);
        columns.add(innerColumn);
        return columns;
    }

    /**
     * Unnest queries in HAVING clause if any.
     *
     * @param plainSelect treat HAVING clause of this query
     */
    void treatHaving(PlainSelect plainSelect) {
        Expression originalHaving = plainSelect.getHaving();
        if (originalHaving != null) {
            originalHaving.accept(this);
            plainSelect.setHaving(exprStack.pop());
        }
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        // Initialize new scope and new alias-to-column mapping
        Set<ColumnRef> newScopeCols = new HashSet<>();
        scopeCols.push(newScopeCols);
        Map<String, List<String>> curAliasToCols = new HashMap<>();
        aliasToCols.push(curAliasToCols);
        List<Table> curAddToFrom = new ArrayList<>();
        addToFrom.push(curAddToFrom);
        Set<Expression> unary = new HashSet<>();
        if (unaryPredicates.size() > 0) {
            Set<Expression> outer = unaryPredicates.peek();
            unary.addAll(outer);
            outer.clear();
        }
        unaryPredicates.push(unary);
        Set<String> temporary = new HashSet<>();
        temporaryTables.add(temporary);
        // Treat base tables in FROM clause
        treatSimpleFrom(plainSelect);
        // Treat sub-queries in FROM clause
        treatNestedFrom(plainSelect);
        // Resolve wildcard in SELECT clause if any
        resolveWildcards(plainSelect);
        // Unnest sub-queries in WHERE clause
        treatWhere(plainSelect);
        // Unnest sub-queries in HAVING clause
        treatHaving(plainSelect);
        // Add unnested sub-queries to FROM clause if any
        expandFrom(plainSelect);
        // Tentatively add non-local predicates from
        // nested queries to this WHERE clause.
        addNonLocalPreds(plainSelect);
        // Tentatively add unary predicates from
        // nested queries to this WHERE clause.
//		addUnaryPreds(plainSelect);
        // Single out predicates referencing outer scope
        separateNonLocalPreds(plainSelect);
        // Add unnested query to query sequence
        unnestedQueries.add(plainSelect);
        // Register names of sub-query result fields
        registerResultCols(plainSelect);
        // Remove new outer scope
        scopeCols.pop();
        // Remove alias to column mapping
        aliasToCols.pop();
        // Remove tables to add to from clause
        addToFrom.pop();
    }

    @Override
    public void visit(SetOperationList setOpList) {
        sqlExceptions.add(new SQLexception("Error - "
                + "set operations are currently not supported"));
    }

    @Override
    public void visit(WithItem withItem) {
        sqlExceptions.add(new SQLexception("Error - "
                + "'WITH' clauses are currently not supported"));
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
                PlainSelect plainSelect = (PlainSelect) selectBody;
                // Rewrite sub-query and add to query list
                Table resultTable = new Table(alias);
                plainSelect.setIntoTables(Arrays.asList(
                        new Table[]{resultTable}));
                plainSelect.accept(this);
                // Replace nested sub-query by table reference
                List<String> subqueryCols = subqueryFields.pop();
                String firstCol = subqueryCols.get(0);
                exprStack.push(new Column(resultTable, firstCol));
                // Add sub-query fields to scope
                Set<ColumnRef> curScope = scopeCols.peek();
                for (String subQueryCol : subqueryCols) {
                    // Only allow fully qualified references -
                    // the only references should be generated
                    // during unnesting anyway.
                    //curScope.add(new ColumnRef("", subQueryCol));
                    curScope.add(new ColumnRef(alias, subQueryCol));
                }
                // Schedule table containing sub-query result to
                // be added to FROM clause.
                addToFrom.peek().add(resultTable);
            } else {
                sqlExceptions.add(new SQLexception("Error - "
                        + "unsupported sub-query type: "
                        + selectBody + " (type: " +
                        selectBody.getClass() + ")"));
            }
        }
    }
}
