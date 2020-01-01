package expressions;

import java.util.*;

import buffer.BufferManager;
import config.LoggingConfig;
import data.ColumnData;
import data.IntData;
import indexing.Index;
import joining.parallel.indexing.PartitionIndex;
import net.sf.jsqlparser.schema.Column;
import preprocessing.Context;
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
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

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
	public final Map<Integer, ColumnData> dataMentioned;
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
	 * Unique id of predicate.
	 */
	public int pid;
	/**
	 * Type of columns that gets involved with predicate.
	 */
	public JavaType type;
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
	 * Construct a map from tables to index and data.
	 *
	 * @param preSummary	query processing context.
	 */
	public void extractIndex(Context preSummary) {
		// Get table indices of join columns
		for (ColumnRef col : this.columnsMentioned) {
			int table = queryInfo.aliasToIndex.get(col.aliasName);
			ColumnRef columnRef = preSummary.columnMapping.get(col);
			ColumnData data = null;
			try {
				data = BufferManager.getData(columnRef);
			} catch (Exception e) {
				e.printStackTrace();
			}
			Index index = BufferManager.colToIndex.get(columnRef);
			indexMentioned.put(table, index);
			dataMentioned.put(table, data);
		}
	}

	/**
	 * Get the column type.
	 */
	public void setColumnType() {
		for (Map.Entry<Expression, SQLtype> entry: expressionToType.entrySet()) {
			if (entry.getKey() instanceof Column) {
				type = TypeUtil.toJavaType(entry.getValue());
				break;
			}
		}
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
