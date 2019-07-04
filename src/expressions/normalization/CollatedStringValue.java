package expressions.normalization;

import net.sf.jsqlparser.expression.StringValue;

/**
 * Represents String value with associated collation (determining
 * string sort order in Postgres).
 * 
 * @author immanueltrummer
 *
 */
public class CollatedStringValue extends StringValue {
	/**
	 * Collation to use.
	 */
	final String collation;
	/**
	 * Initializes string value for given string and collation.
	 * 
	 * @param escapedValue	text
	 * @param collation		collation to use for sorting
	 */
	public CollatedStringValue(String escapedValue, String collation) {
		super(escapedValue);
		this.collation = collation;
	}
	@Override
	public String toString() {
		return "(" + super.toString() + " COLLATE \"" + collation + "\")"; 
	}
}
