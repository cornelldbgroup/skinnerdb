package expressions.normalization;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.cnfexpression.MultiAndExpression;
import net.sf.jsqlparser.util.cnfexpression.MultiOrExpression;

import java.util.*;

public class RedundantExpressionRemovalVisitor extends CopyVisitor {
    private boolean shouldConvertToMulti;

    public RedundantExpressionRemovalVisitor(boolean multiConversion) {
        this.shouldConvertToMulti = multiConversion;
    }

    @Override
    public void visit(AndExpression andExpression) {
        Map<String, Long> maxConstraint = new HashMap<>();
        Map<String, Long> minConstraint = new HashMap<>();

        List<Expression> conjuncts = new ArrayList<>();

        Stack<Expression> conjunctStack = new Stack<>();
        conjunctStack.push(andExpression.getLeftExpression());
        conjunctStack.push(andExpression.getRightExpression());
        while (!conjunctStack.empty()) {
            Expression original = conjunctStack.pop();
            Expression curr = original;
            while (curr instanceof Parenthesis) {
                curr = ((Parenthesis) curr).getExpression();
            }


            if (curr instanceof AndExpression) {
                AndExpression conjunction = (AndExpression) curr;
                conjunctStack.push(conjunction.getLeftExpression());
                conjunctStack.push(conjunction.getRightExpression());
            } else {
                conjuncts.add(original);
                if (curr instanceof BinaryExpression) {
                    BinaryExpression cmp = (BinaryExpression) curr;
                    Expression left = cmp.getLeftExpression();
                    Expression right = cmp.getRightExpression();

                    if (!(left instanceof Column)) {
                        continue;
                    }
                    Column column = (Column) left;
                    String key = column.getFullyQualifiedName();

                    if (!(right instanceof LongValue)) {
                        continue;
                    }
                    LongValue constant = (LongValue) right;
                    Long value = constant.getValue();


                    if (cmp instanceof GreaterThanEquals) {
                        long currentConstraint =
                                minConstraint.containsKey(key) ?
                                        minConstraint.get(key) :
                                        Long.MIN_VALUE;
                        long newConstraint =
                                Math.max(currentConstraint,
                                        value == Long.MIN_VALUE ? value :
                                                value - 1);
                        minConstraint.put(key, newConstraint);
                    } else if (cmp instanceof GreaterThan) {
                        long currentConstraint =
                                minConstraint.containsKey(key) ?
                                        minConstraint.get(key) :
                                        Long.MIN_VALUE;
                        long newConstraint =
                                Math.max(currentConstraint, value);
                        minConstraint.put(key, newConstraint);
                    } else if (cmp instanceof MinorThanEquals) {
                        long currentConstraint =
                                maxConstraint.containsKey(key) ?
                                        maxConstraint.get(key) :
                                        Long.MAX_VALUE;
                        long newConstraint =
                                Math.min(currentConstraint,
                                        value == Long.MAX_VALUE ? value :
                                                value + 1);
                        maxConstraint.put(key, newConstraint);
                    } else if (cmp instanceof MinorThan) {
                        long currentConstraint =
                                maxConstraint.containsKey(key) ?
                                        maxConstraint.get(key) :
                                        Long.MAX_VALUE;
                        long newConstraint =
                                Math.min(currentConstraint, value);
                        maxConstraint.put(key, newConstraint);
                    }
                }
            }
        }

        List<Expression> filtered = new ArrayList<>();
        for (Expression original : conjuncts) {
            Expression curr = original;
            while (curr instanceof Parenthesis) {
                curr = ((Parenthesis) curr).getExpression();
            }

            if (!(curr instanceof BinaryExpression)) {
                filtered.add(original);
                continue;
            }

            BinaryExpression cmp = (BinaryExpression) curr;
            Expression left = cmp.getLeftExpression();
            Expression right = cmp.getRightExpression();

            if (!(left instanceof Column)) {
                filtered.add(original);
                continue;
            }
            Column column = (Column) left;
            String key = column.getFullyQualifiedName();

            if (!(right instanceof LongValue)) {
                filtered.add(original);
                continue;
            }
            LongValue constant = (LongValue) right;
            Long value = constant.getValue();


            if (cmp instanceof GreaterThanEquals &&
                    minConstraint.containsKey(key) &&
                    minConstraint.get(key) == value - 1) {
                filtered.add(original);
                minConstraint.remove(key);
                continue;
            }

            if (cmp instanceof GreaterThan &&
                    minConstraint.containsKey(key) &&
                    minConstraint.get(key) == value) {
                filtered.add(original);
                minConstraint.remove(key);
                continue;
            }

            if (cmp instanceof MinorThanEquals &&
                    maxConstraint.containsKey(key) &&
                    maxConstraint.get(key) == value + 1) {
                filtered.add(original);
                maxConstraint.remove(key);
                continue;
            }

            if (cmp instanceof MinorThan &&
                    maxConstraint.containsKey(key) &&
                    maxConstraint.get(key) == value) {
                filtered.add(original);
                maxConstraint.remove(key);
                continue;
            }
        }

        List<Expression> visited = new ArrayList<>();
        for (Expression expression : filtered) {
            expression.accept(this);
            visited.add(exprStack.pop());
        }

        if (shouldConvertToMulti) {
            MultiAndExpression and = new MultiAndExpression(visited);
            exprStack.push(and);
        } else {
            Expression prev = null;

            for (Expression exp : visited) {
                if (prev != null) {
                    prev = new AndExpression(exp, prev);
                } else {
                    prev = exp;
                }
            }

            exprStack.push(prev);
        }
    }

    @Override
    public void visit(OrExpression orExpression) {
        Map<String, Long> maxConstraint = new HashMap<>();
        Map<String, Long> minConstraint = new HashMap<>();

        List<Expression> disjuncts = new ArrayList<>();

        Stack<Expression> disjunctStack = new Stack<>();
        disjunctStack.push(orExpression.getRightExpression());
        disjunctStack.push(orExpression.getLeftExpression());
        while (!disjunctStack.empty()) {
            Expression original = disjunctStack.pop();
            Expression curr = original;
            while (curr instanceof Parenthesis) {
                curr = ((Parenthesis) curr).getExpression();
            }


            if (curr instanceof OrExpression) {
                OrExpression disjunction = (OrExpression) curr;
                disjunctStack.push(disjunction.getRightExpression());
                disjunctStack.push(disjunction.getLeftExpression());
                continue;
            } else {
                disjuncts.add(original);
                if (curr instanceof BinaryExpression) {
                    BinaryExpression cmp = (BinaryExpression) curr;
                    Expression left = cmp.getLeftExpression();
                    Expression right = cmp.getRightExpression();

                    if (!(left instanceof Column)) {
                        continue;
                    }
                    Column column = (Column) left;
                    String key = column.getFullyQualifiedName();

                    if (!(right instanceof LongValue)) {
                        continue;
                    }
                    LongValue constant = (LongValue) right;
                    Long value = constant.getValue();


                    if (cmp instanceof GreaterThanEquals) {
                        long currentConstraint =
                                minConstraint.containsKey(key) ?
                                        minConstraint.get(key) :
                                        Long.MAX_VALUE;
                        long newConstraint =
                                Math.min(currentConstraint, value - 1);
                        minConstraint.put(key, newConstraint);
                    } else if (cmp instanceof GreaterThan) {
                        long currentConstraint =
                                minConstraint.containsKey(key) ?
                                        minConstraint.get(key) :
                                        Long.MAX_VALUE;
                        long newConstraint =
                                Math.min(currentConstraint, value);
                        minConstraint.put(key, newConstraint);
                    } else if (cmp instanceof MinorThanEquals) {
                        long currentConstraint =
                                maxConstraint.containsKey(key) ?
                                        maxConstraint.get(key) :
                                        Long.MIN_VALUE;
                        long newConstraint =
                                Math.max(currentConstraint, value + 1);
                        maxConstraint.put(key, newConstraint);
                    } else if (cmp instanceof MinorThan) {
                        long currentConstraint =
                                maxConstraint.containsKey(key) ?
                                        maxConstraint.get(key) :
                                        Long.MIN_VALUE;
                        long newConstraint =
                                Math.max(currentConstraint, value);
                        maxConstraint.put(key, newConstraint);
                    }
                }
            }
        }

        List<Expression> filtered = new ArrayList<>();
        for (Expression original : disjuncts) {
            Expression curr = original;
            while (curr instanceof Parenthesis) {
                curr = ((Parenthesis) curr).getExpression();
            }

            if (!(curr instanceof BinaryExpression)) {
                filtered.add(original);
                continue;
            }

            BinaryExpression cmp = (BinaryExpression) curr;
            Expression left = cmp.getLeftExpression();
            Expression right = cmp.getRightExpression();

            if (!(left instanceof Column)) {
                filtered.add(original);
                continue;
            }
            Column column = (Column) left;
            String key = column.getFullyQualifiedName();

            if (!(right instanceof LongValue)) {
                filtered.add(original);
                continue;
            }
            LongValue constant = (LongValue) right;
            Long value = constant.getValue();


            if (cmp instanceof GreaterThanEquals &&
                    minConstraint.containsKey(key) &&
                    minConstraint.get(key) == value - 1) {
                filtered.add(original);
                minConstraint.remove(key);
                continue;
            }

            if (cmp instanceof GreaterThan &&
                    minConstraint.containsKey(key) &&
                    minConstraint.get(key) == value) {
                filtered.add(original);
                minConstraint.remove(key);
                continue;
            }

            if (cmp instanceof MinorThanEquals &&
                    maxConstraint.containsKey(key) &&
                    maxConstraint.get(key) == value + 1) {
                filtered.add(original);
                maxConstraint.remove(key);
                continue;
            }

            if (cmp instanceof MinorThan &&
                    maxConstraint.containsKey(key) &&
                    maxConstraint.get(key) == value) {
                filtered.add(original);
                maxConstraint.remove(key);
                continue;
            }
        }

        List<Expression> visited = new ArrayList<>();
        for (Expression expression : filtered) {
            expression.accept(this);
            visited.add(exprStack.pop());
        }

        if (shouldConvertToMulti) {
            MultiOrExpression expression = new MultiOrExpression(visited);
            exprStack.push(expression);
        } else {
            Expression prev = null;

            for (Expression exp : visited) {
                if (prev != null) {
                    prev = new OrExpression(exp, prev);
                } else {
                    prev = exp;
                }
            }

            exprStack.push(prev);
        }
    }
}
