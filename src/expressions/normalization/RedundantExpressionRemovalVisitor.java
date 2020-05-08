package expressions.normalization;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class RedundantExpressionRemovalVisitor extends CopyVisitor {


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

        if (visited.size() == 1) {
            exprStack.push(visited.get(0));
        } else {
            AndExpression conjunction =
                    new AndExpression(visited.get(visited.size() - 2),
                            visited.get(visited.size() - 1));

            for (int i = visited.size() - 3; i >= 0; i--) {
                conjunction = new AndExpression(visited.get(i), conjunction);
            }

            exprStack.push(conjunction);
        }
    }

    /*
    @Override
    public void visit(OrExpression orExpression) {
        Map<String, Long> maxConstraint = new HashMap<>();
        Map<String, Long> minConstraint = new HashMap<>();

        List<Expression> disjuncts = new ArrayList<>();

        Stack<Expression> disjunctStack = new Stack<>();
        disjunctStack.push(orExpression.getLeftExpression());
        disjunctStack.push(orExpression.getRightExpression());
        while (!disjunctStack.empty()) {
            Expression original = disjunctStack.pop();
            Expression curr = original;
            while (curr instanceof Parenthesis) {
                curr = ((Parenthesis) curr).getExpression();
            }


            if (curr instanceof OrExpression) {
                OrExpression disjunction = (OrExpression) curr;
                disjunctStack.push(disjunction.getLeftExpression());
                disjunctStack.push(disjunction.getRightExpression());
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

        if (visited.size() == 1) {
            exprStack.push(visited.get(0));
        } else {
            OrExpression disjunction =
                    new OrExpression(visited.get(visited.size() - 2),
                            visited.get(visited.size() - 1));

            for (int i = visited.size() - 3; i >= 0; i--) {
                disjunction = new OrExpression(visited.get(i), disjunction);
            }

            exprStack.push(disjunction);
        }
    }*/
}
