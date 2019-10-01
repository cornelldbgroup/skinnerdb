package expressions;

import java.util.*;

import buffer.BufferManager;
import config.LoggingConfig;
import data.ColumnData;
import data.IntData;
import indexing.Index;
import preprocessing.Context;
import query.ColumnRef;
import query.QueryInfo;
import expressions.normalization.CollectReferencesVisitor;
import expressions.normalization.NormalizeColumnsVisitor;
import expressions.normalization.SimplificationVisitor;
import expressions.normalization.SubstitutionVisitor;
import expressions.typing.ExpressionScope;
import expressions.typing.TypeVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import types.SQLtype;

/**
 * Stores information on one expression (e.g., a predicate).
 * 
 * @author immanueltrummer
 *
 */
public class ExpressionInfo {
	/**
	 * Contains information about the query surrounding the expression.
	 */
	public final QueryInfo queryInfo;
	/**
	 * The original expression.
	 */
	public final Expression originalExpression;
	/**
	 * Expression after substituting place-holders by expressions.
	 */
	public final Expression afterSubstitution;
	/**
	 * Expression after normalizing column references.
	 */
	public final Expression afterNormalization;
	/**
	 * Expression after inserting type cases if necessary.
	 */
	//public final Expression afterCasting;
	/**
	 * Expression after simplification (e.g., resolving
	 * arithmetic expressions involving only constants).
	 */
	public final Expression finalExpression;
	/**
	 * Aliases of all tables mentioned in the expression.
	 */
	public final Set<String> aliasesMentioned;
	/**
	 * Indices of all table aliases mentioned in this expression.
	 */
	public final Set<Integer> aliasIdxMentioned;
	/**
	 * All columns mentioned in the expression.
	 */
	public final Set<ColumnRef> columnsMentioned;
	/**
	 * All columns mentioned in the expression.
	 */
	public final Map<Integer, Index> indexMentioned;
	/**
	 * All columns mentioned in the expression.
	 */
	public final Map<Integer, IntData> dataMentioned;
	/**
	 * Set of SQL LIKE expressions found in the given expression.
	 */
	public final Set<Expression> likeExpressions;
	/**
	 * Set of aggregation functions with parameters used in given expression.
	 */
	public final Set<Function> aggregates;
	/**
	 * Maps each expression fragment in fully normalized
	 * expression to the associated expression type.
	 */
	public final Map<Expression, SQLtype> expressionToType;
	/**
	 * Maps each expression fragment in fully normalized
	 * expression to the associated expression scope.
	 */
	public final Map<Expression, ExpressionScope> expressionToScope;
	/**
	 * Type of expression (as a whole).
	 */
	public final SQLtype resultType;
	/**
	 * Scope of expression (as a whole).
	 */
	public final ExpressionScope resultScope;
	/**
	 * Decomposes expression into conjuncts.
	 */
	public final List<Expression> conjuncts;


	/**
	 * Extracts all conjuncts from a nested AND expression
	 * via recursive calls. The result will be stored in
	 * the second parameter.
	 * 
	 * @param condition	the remaining condition (no conjuncts extracted yet)
	 * @param conjuncts	stores the resulting conjuncts
	 */
	static void extractConjuncts(Expression condition, 
			List<Expression> conjuncts) {
		if (condition instanceof AndExpression) {
			AndExpression and = (AndExpression)condition;
			extractConjuncts(and.getLeftExpression(), conjuncts);
			extractConjuncts(and.getRightExpression(), conjuncts);
		} else {
			conjuncts.add(condition);
		}
	}
	/**
	 * Visits the given expression using the given visitor within a try-catch
	 * block. Check whether visitor stored the root cause of the exception and
	 * throw corresponding exception in that case. Otherwise throw original
	 * exception (or execute peacefully). This is needed since the original
	 * expression visitor interface does not consider exceptions.
	 * 
	 * @param expression	expression to visit
	 * @param visitor		visit expression via this visitor
	 * @throws Exception
	 */
	void tryVisit(Expression expression, SkinnerVisitor visitor) throws Exception {
		try {
			expression.accept(visitor);
		} catch (Exception e) {
			// Was exception caused by another exception that
			// was not yet thrown?
			if (!visitor.sqlExceptions.isEmpty()) {
				throw visitor.sqlExceptions.get(0);
			} else {
				throw e;
			}
		}
		// Check whether any errors occurred -
		// throw corresponding exceptions if so.
		if (!visitor.sqlExceptions.isEmpty()) {
			throw visitor.sqlExceptions.get(0);
		}
	}

	public void extractIndex(Context preSummary) {
		// Get table indices of join columns
		for (ColumnRef col : this.columnsMentioned) {
			int table = queryInfo.aliasToIndex.get(col.aliasName);
			ColumnRef columnRef = preSummary.columnMapping.get(col);
			IntData data = null;
			try {
				data = (IntData) BufferManager.getData(columnRef);
			} catch (Exception e) {
				e.printStackTrace();
			}
			Index index = BufferManager.colToIndex.get(columnRef);

			indexMentioned.put(table, index);
			dataMentioned.put(table, data);
		}
	}
	/**
	 * Initializes the expression info.
	 * 
	 * @param queryInfo		meta-data about input query
	 * @param expression	the expression
	 */
	public ExpressionInfo(QueryInfo queryInfo, 
			Expression expression) throws Exception {
		// Store input parameters
		this.queryInfo = queryInfo;
		this.originalExpression = expression;
		log("Original:\t" + originalExpression.toString());
		// Substitute references to expressions in SELECT clause -
		// TODO: remove this.
		SubstitutionVisitor substitutionVisitor = 
				new SubstitutionVisitor(queryInfo.aliasToExpression);
		tryVisit(originalExpression, substitutionVisitor);
		this.afterSubstitution = substitutionVisitor.exprStack.pop();
		log("Substituted:\t" + afterSubstitution);		
		// Complete column references by inferring table aliases
		NormalizeColumnsVisitor normalizationVisitor =
				new NormalizeColumnsVisitor(queryInfo.columnToAlias);
		tryVisit(afterSubstitution, normalizationVisitor);
		this.afterNormalization = normalizationVisitor.exprStack.pop();
		log("Normalized:\t" + afterNormalization);
		// Makes implicit type casts explicit
		TypeVisitor typeVisitor = new TypeVisitor(queryInfo);
		tryVisit(afterNormalization, typeVisitor);
		// Simplifies the expression by replacing certain SQL constructs
		// and resolving constant expressions.
		SimplificationVisitor simplificationVisitor =
				new SimplificationVisitor();
		tryVisit(afterNormalization, simplificationVisitor);
		this.finalExpression = simplificationVisitor.opStack.pop();
		log("Final:\t" + finalExpression.toString());
		// Collect references in the given expression
		CollectReferencesVisitor collectorVisitor =
				new CollectReferencesVisitor();
		tryVisit(finalExpression, collectorVisitor);
		this.aliasesMentioned = collectorVisitor.mentionedTables;
		this.aliasIdxMentioned = new HashSet<>();
		for (String alias : aliasesMentioned) {
			int idx = queryInfo.aliasToIndex.get(alias);
			aliasIdxMentioned.add(idx);
		}
		this.columnsMentioned = collectorVisitor.mentionedColumns;
		this.indexMentioned = new HashMap<>();
		this.dataMentioned = new HashMap<>();
		this.likeExpressions = collectorVisitor.likeExpressions;
		this.aggregates = collectorVisitor.aggregates;
		log("Aliases:\t" + aliasesMentioned.toString());
		log("Aliase idx:\t" + aliasIdxMentioned.toString());
		log("Columns:\t" + columnsMentioned.toString());
		log("Like expressions:\t" + likeExpressions.toString());
		log("Aggregates: " + aggregates.toString());
		// Final typing
		tryVisit(finalExpression, typeVisitor);
		this.expressionToType = typeVisitor.outputType;
		this.resultType = expressionToType.get(finalExpression);
		this.expressionToScope = typeVisitor.outputScope;
		this.resultScope = expressionToScope.get(finalExpression);
		log("Expression types:\t" + expressionToType.toString());
		log("Expression scopes:\t" + expressionToScope.toString());
		// Extract conjuncts
		conjuncts = new ArrayList<Expression>();
		extractConjuncts(finalExpression, conjuncts);
		log("Conjuncts:\t" + conjuncts.toString());
	}
	/**
	 * Outputs given string if expression logging
	 * is activated.
	 * 
	 * @param logEntry	entry to log
	 */
	void log(String logEntry) {
		if (LoggingConfig.EXPRESSIONS_VERBOSE) {
			System.out.println(logEntry);
		}
	}
	@Override
	public String toString() {
		return finalExpression.toString();
	}
}
