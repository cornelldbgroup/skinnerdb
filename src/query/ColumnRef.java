package query;

/**
 * Represents a reference to a specific column
 * within a specific table alias.
 * 
 * @author immanueltrummer
 *
 */
public class ColumnRef {
	/**
	 * Name of the table alias (unique within query).
	 */
	public final String aliasName;
	/**
	 * Name of the column (unique within table alias).
	 */
	public final String columnName;
	/**
	 * Initializes reference for given column and table alias.
	 * 
	 * @param aliasName		name of table alias
	 * @param columnName	name of the column
	 */
	public ColumnRef(String aliasName, String columnName) {
		this.aliasName = aliasName.toLowerCase();
		this.columnName = columnName.toLowerCase();
	}
	@Override
	public boolean equals(Object other) {
		if (other instanceof ColumnRef) {
			ColumnRef otherRef = (ColumnRef)other;
			return columnName.equalsIgnoreCase(otherRef.columnName) &&
					aliasName.equalsIgnoreCase(otherRef.aliasName);
		} else {
			return false;
		}
	}
	@Override
	public int hashCode() {
		return columnName.hashCode() + aliasName.hashCode();
	}
	@Override
	public String toString() {
		return aliasName + "." + columnName;
	}
	/**
	 * String representation with customized separator
	 * between alias name and column name.
	 * 
	 * @param separator	separates alias from column name
	 * @return			column reference with custom separator
	 */
	public String toString(String separator) {
		return aliasName + separator + columnName;
	}
}