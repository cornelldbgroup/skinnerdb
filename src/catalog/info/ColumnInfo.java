package catalog.info;

import java.io.Serializable;

import types.SQLtype;

/**
 * Meta-data about one table column.
 * 
 * @author immanueltrummer
 *
 */
public class ColumnInfo implements Serializable {
	/**
	 * Name of the column.
	 */
	public final String name;
	/**
	 * SQL type of column.
	 */
	public final SQLtype type;
	/**
	 * Whether the column is part of the primary key.
	 */
	public final boolean isPrimary;
	/**
	 * Whether values in the column are unique.
	 */
	public final boolean isUnique;
	/**
	 * Whether column values cannot be null.
	 */
	public final boolean isNotNull;
	/**
	 * Whether the column contains foreign key references.
	 */
	public final boolean isForeign;
	/**
	 * Initializes column with given name, type, and data path.
	 * 
	 * @param name		column name
	 * @param type		column data type
	 * @param isPrimary	whether the column is part of the primary key
	 * @param isUnique	whether values in the column are unique
	 * @param isNotNull	whether column values cannot be null
	 * @param isForeign	whether the column contains foreign key references
	 */
	public ColumnInfo(String name, SQLtype type, boolean isPrimary, 
			boolean isUnique, boolean isNotNull, boolean isForeign) {
		this.name = name;
		this.type = type;
		this.isPrimary = isPrimary;
		this.isUnique = isUnique;
		this.isNotNull = isNotNull;
		this.isForeign = isForeign;
	}
	@Override
	public String toString() {
		return name + " " + type;
	}
}
