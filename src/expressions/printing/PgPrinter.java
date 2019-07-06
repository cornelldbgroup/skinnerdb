package expressions.printing;

import catalog.info.ColumnInfo;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import query.ColumnRef;
import query.QueryInfo;
import types.SQLtype;

/**
 * Prints out expressions while adding collation instructions
 * for Postgres. This is used to test consistency between
 * SkinnerDB and Postgres.
 * 
 * @author immanueltrummer
 *
 */
public class PgPrinter extends ExpressionDeParser {
	/**
	 * Query to process.
	 */
	final QueryInfo query;
	/**
	 * Initializes for specific query.
	 * 
	 * @param query	maps query to DB columns
	 */
	public PgPrinter(QueryInfo query) {
		this.query = query;
	}
	@Override
	public void visit(StringValue string) {
		this.getBuffer().append("(");
		this.getBuffer().append(string.toString());
		this.getBuffer().append(" COLLATE \"C\") ");
	}
	@Override
	public void visit(Column column) {
		// Check whether this is a string column
		String tblName = column.getTable().getName();
		String colName = column.getColumnName();
		ColumnRef queryRef = new ColumnRef(tblName, colName);
		ColumnInfo colInfo = query.colRefToInfo.get(queryRef);
		if (colInfo.type.equals(SQLtype.STRING) ||
				colInfo.type.equals(SQLtype.STRING_CODE)) {
			this.getBuffer().append("(");
			this.getBuffer().append(column.toString());
			this.getBuffer().append(" COLLATE \"C\") ");
		} else {
			this.getBuffer().append(column.toString());
		}
	}
}
