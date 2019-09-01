package types;

/**
 * Contains utility methods for dealing with types.
 * 
 * @author immanueltrummer
 *
 */
public class TypeUtil {
	/**
	 * Returns the Java type that Skinner uses internally
	 * to represent SQL expression types.
	 * 
	 * @param expressionType	expression SQL type
	 * @return					corresponding Java type
	 */
	public static JavaType toJavaType(SQLtype expressionType) {
		switch (expressionType) {
		case BOOL:
		case BYTE:
		case INT:
		case STRING_CODE:
		case DATE:
		case TIME:
		case TIMESTAMP:
		case YM_INTERVAL:
		case DT_INTERVAL:
			return JavaType.INT;
		case LONG:
			return JavaType.LONG;
		case DOUBLE:
			return JavaType.DOUBLE;
		case CHAR:
		case STRING:
			return JavaType.STRING;
		default:
			return null;
		}
	}
	/**
	 * Returns true iff the first type specializes the second one.
	 * 
	 * @param type1		potentially specializing type
	 * @param type2		potentially generalizing type
	 * @return			true if first type specializes the second
	 */
	public static boolean specializes(SQLtype type1, 
			SQLtype type2) {
		switch (type1) {
		case BYTE:
			// Integer, long, or double "generalize" byte
			return type2 == SQLtype.INT || 
				type2 == SQLtype.LONG ||
				type2 == SQLtype.DOUBLE;
		case INT:
			// Long or double "generalizes" integer
			return type2 == SQLtype.LONG ||
				type2 == SQLtype.DOUBLE;
		case STRING_CODE:
			// String generalizes strings in dictionary
			return type2 == SQLtype.STRING;
		default:
			return false;
		}
	}
	/**
	 * Returns the most specific type that covers both inputs
	 * or null if no such type exists.
	 * 
	 * @param type1		first input expression type
	 * @param type2		second input expression type
	 * @return			most specific common type
	 */
	public static SQLtype commonType(SQLtype type1, 
			SQLtype type2) {
		if (type1 == null || type2 == null) {
			return null;
		} else if (type1.equals(type2)) {
			return type1;
		} else if (specializes(type1, type2)) {
			return type2;
		} else if (specializes(type2, type1)) {
			return type1;
		} else if (type1.equals(SQLtype.BOOL) && type2.equals(SQLtype.INT) ||
				type2.equals(SQLtype.BOOL) && type1.equals(SQLtype.INT)) {
			// Takes into account that Boolean constants are currently
			// represented as integer values.
			return SQLtype.BOOL;
		} else {
			for (SQLtype type : new SQLtype[] {
					SQLtype.BYTE, 
					SQLtype.INT, 
					SQLtype.LONG,
					SQLtype.DOUBLE
			}) {
				if (specializes(type1, type) && specializes(type2, type)) {
					return type;
				}
			}
			return null;
		}
	}
	/**
	 * Returns true iff the given type is one of
	 * the data types representing time intervals.
	 * 
	 * @param type	test whether this type is interval type
	 * @return		true iff given type represents time interval
	 */
	public static boolean isInterval(SQLtype type) {
		return type.equals(SQLtype.DT_INTERVAL) ||
				type.equals(SQLtype.YM_INTERVAL);
	}
	/**
	 * Returns corresponding expression type for a given name.
	 * 
	 * @param typeString	string representation of type
	 * @return				corresponding expression type
	 */
	public static SQLtype parseString(String typeString) {
		switch (typeString.toLowerCase()) {
		case "bool":
			return SQLtype.BOOL;
		case "byte":
			return SQLtype.BYTE;
		case "int":
		case "integer":
			return SQLtype.INT;
		case "long":
			return SQLtype.LONG;
		case "double":
		case "numeric":
			return SQLtype.DOUBLE;
		case "stringcode":
		case "string_code":
			return SQLtype.STRING_CODE;
		case "character":
		case "char":
		case "character varying":
		case "varchar":
		case "text":
		case "string":
			return SQLtype.STRING;
		case "date":
			return SQLtype.DATE;
		case "time":
			return SQLtype.TIME;
		case "timestamp":
			return SQLtype.TIMESTAMP;
		default:
			return null;
		}
	}
}
