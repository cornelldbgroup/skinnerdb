package benchmark;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.Random;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import query.where.WhereUtil;

public class TransformQueries {
	public static void main(String[] args) throws Exception {
		// Extract command line parameters
		if (args.length < 3) {
			System.err.println("Error - specify input and "
					+ "output directory and predicate "
					+ "transformation probability!");
			return;
		}
		String inputPath = args[0];
		String outputDir = args[1];
		double changeProb = Double.parseDouble(args[2]);
		// Read all input queries
		Map<String, PlainSelect> nameToSelect = 
				BenchUtil.readAllQueries(inputPath);
		// Iterate over queries to transform
		Random random = new Random();
		for (Entry<String, PlainSelect> entry : nameToSelect.entrySet()) {
			String queryName = entry.getKey();
			PlainSelect query = entry.getValue();
			//QueryInfo info = new QueryInfo(query, false, -1, -1, null);
			List<Expression> oldPreds = new ArrayList<>();
			WhereUtil.extractConjuncts(query.getWhere(), oldPreds);
			// Iterate over all unary predicates
			List<Expression> newPreds = new ArrayList<>();
			for (Expression oldPred : oldPreds) {
				boolean predReplaced = false;
				if (oldPred instanceof EqualsTo) {
					EqualsTo equals = (EqualsTo)oldPred;
					Expression left = equals.getLeftExpression();
					Expression right = equals.getRightExpression();
					Expression constExpr = null;
					Expression columnExpr = null;
					if (left instanceof Column && 
							right instanceof StringValue) {
						constExpr = right;
						columnExpr = left;
					} else if (left instanceof StringValue && 
							right instanceof Column) {
						constExpr = left;
						columnExpr = right;
					}
					if (constExpr != null) {
						if (constExpr instanceof StringValue) {
							if (random.nextDouble()<changeProb) {
								/*
								EqualsTo notEquals = new EqualsTo();
								notEquals.setNot();
								notEquals.setLeftExpression(columnExpr);
								String constStr = ((StringValue) constExpr).getValue();
								StringValue notValue = new StringValue(constStr + "XXX");
								notEquals.setRightExpression(notValue);
								newPreds.add(notEquals);
								*/
								Function eqlFun = new Function();
								eqlFun.setName("eqlFun");
								List<Expression> eqlList = new ArrayList<>();
								eqlList.add(columnExpr);
								eqlList.add(constExpr);
								ExpressionList eqlExpList = new ExpressionList(eqlList);
								eqlFun.setParameters(eqlExpList);
								if (equals.isNot()) {
									NotExpression not = 
											new NotExpression(eqlFun);
									newPreds.add(not);
								} else {
									newPreds.add(eqlFun);
								}
								predReplaced = true;
							}
						}
					}
				}
				if (!predReplaced) {
					newPreds.add(oldPred);
				}
			}
			// Update query with new predicates
			query.setWhere(WhereUtil.conjunction(newPreds));
			// Generate output file
			String outputPath = outputDir + "/T" + queryName;
			File queryFile = new File(outputPath);
			PrintWriter queryWriter = new PrintWriter(queryFile);
			queryWriter.println("-- Change probability: " + changeProb);
			// Output query to file
			queryWriter.println(query.toString() + ";");
			queryWriter.println();
			// Close output file
			queryWriter.close();
		}
	}
}
