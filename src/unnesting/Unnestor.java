package unnesting;

import java.util.ArrayList;
import java.util.List;

import expressions.normalization.PlainVisitor;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;
import query.QueryInfo;

public class Unnestor extends PlainVisitor 
	implements SelectVisitor, FromItemVisitor {

	public List<QueryInfo> simpleQueries = new ArrayList<>();
	
	@Override
	public void visit(PlainSelect plainSelect) {
		
	}

	@Override
	public void visit(SetOperationList setOpList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithItem withItem) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Table tableName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect subSelect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubJoin subjoin) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LateralSubSelect lateralSubSelect) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValuesList valuesList) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TableFunction tableFunction) {
		// TODO Auto-generated method stub
		
	}

}
