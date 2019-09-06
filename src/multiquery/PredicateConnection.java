package multiquery;

import net.sf.jsqlparser.expression.Expression;

import java.util.List;

public class PredicateConnection {

    private Integer leftTableIdx;

    private String leftTableName;

    private Integer rightTableIdx;

    private String rightTableName;

    private List<Expression> unaryExpression;

    public Integer getLeftTableIdx() {
        return leftTableIdx;
    }

    public void setLeftTableIdx(Integer leftTableIdx) {
        this.leftTableIdx = leftTableIdx;
    }

    public String getLeftTableName() {
        return leftTableName;
    }

    public void setLeftTableName(String leftTableName) {
        this.leftTableName = leftTableName;
    }

    public Integer getRightTableIdx() {
        return rightTableIdx;
    }

    public void setRightTableIdx(Integer rightTableIdx) {
        this.rightTableIdx = rightTableIdx;
    }

    public String getRightTableName() {
        return rightTableName;
    }

    public void setRightTableName(String rightTableName) {
        this.rightTableName = rightTableName;
    }

    public List<Expression> getUnaryExpression() {
        return unaryExpression;
    }

    public void setUnaryExpression(List<Expression> unaryExpression) {
        this.unaryExpression = unaryExpression;
    }
}
