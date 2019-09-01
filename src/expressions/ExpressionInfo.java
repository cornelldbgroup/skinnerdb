package expressions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import config.LoggingConfig;
import query.ColumnRef;
import query.QueryInfo;
import query.where.WhereUtil;
import expressions.normalization.CollectReferencesVisitor;
import expressions.normalization.NormalizeColumnsVisitor;
import expressions.normalization.SimplificationVisitor;
import expressions.normalization.SubstitutionVisitor;
import expressions.typing.ExpressionScope;
import expressions.typing.TypeVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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
		VisitorUtil.tryVisit(originalExpression, substitutionVisitor);
		this.afterSubstitution = substitutionVisitor.exprStack.pop();
		log("Substituted:\t" + afterSubstitution);		
		// Complete column references by inferring table aliases
		NormalizeColumnsVisitor normalizationVisitor =
				new NormalizeColumnsVisitor(queryInfo.columnToAlias);
		VisitorUtil.tryVisit(afterSubstitution, normalizationVisitor);
		this.afterNormalization = normalizationVisitor.exprStack.pop();
		log("Normalized:\t" + afterNormalization);
		// Makes implicit type casts explicit
		TypeVisitor typeVisitor = new TypeVisitor(queryInfo);
		VisitorUtil.tryVisit(afterNormalization, typeVisitor);
		// Simplifies the expression by replacing certain SQL constructs
		// and resolving constant expressions.
		SimplificationVisitor simplificationVisitor =
				new SimplificationVisitor();
		VisitorUtil.tryVisit(afterNormalization, simplificationVisitor);
		this.finalExpression = simplificationVisitor.opStack.pop();
		log("Final:\t" + finalExpression.toString());
		// Collect references in the given expression
		CollectReferencesVisitor collectorVisitor =
				new CollectReferencesVisitor();
		VisitorUtil.tryVisit(finalExpression, collectorVisitor);
		this.aliasesMentioned = collectorVisitor.mentionedTables;
		this.aliasIdxMentioned = new HashSet<>();
		for (String alias : aliasesMentioned) {
			int idx = queryInfo.aliasToIndex.get(alias);
			aliasIdxMentioned.add(idx);
		}
		this.columnsMentioned = collectorVisitor.mentionedColumns;
		this.likeExpressions = collectorVisitor.likeExpressions;
		this.aggregates = collectorVisitor.aggregates;
		log("Aliases:\t" + aliasesMentioned.toString());
		log("Aliase idx:\t" + aliasIdxMentioned.toString());
		log("Columns:\t" + columnsMentioned.toString());
		log("Like expressions:\t" + likeExpressions.toString());
		log("Aggregates: " + aggregates.toString());
		// Final typing
		VisitorUtil.tryVisit(finalExpression, typeVisitor);
		this.expressionToType = typeVisitor.outputType;
		this.resultType = expressionToType.get(finalExpression);
		// Final scoping - generic scope defaults to per-tuple scope
		this.expressionToScope = typeVisitor.outputScope;
		ExpressionScope curResultScope = expressionToScope.get(finalExpression);
		this.resultScope = curResultScope.equals(ExpressionScope.ANY_SCOPE)?
				ExpressionScope.PER_TUPLE:curResultScope;
		log("Expression types:\t" + expressionToType.toString());
		log("Expression scopes:\t" + expressionToScope.toString());
		// Extract conjuncts
		conjuncts = new ArrayList<Expression>();
		WhereUtil.extractConjuncts(finalExpression, conjuncts);
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
