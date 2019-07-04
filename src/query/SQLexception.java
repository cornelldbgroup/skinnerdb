package query;

/**
 * An exception caused by an error in the input query
 * (or DDL statement).
 * 
 * @author immanueltrummer
 *
 */
public class SQLexception extends Exception {
	/**
	 * Initialize exception with error description.
	 * 
	 * @param text	text to show to users
	 */
	public SQLexception(String text) {
		super(text);
	}
}
