package query.from;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * Auxiliary functions for analyzing the FROM clause.
 * 
 * @author immanueltrummer
 *
 */
public class FromUtil {
	/**
	 * Extracts all from items for given (simple) query.
	 * 
	 * @param plainSelect	simple query
	 * @return				list of all items in the query's FROM clause
	 */
	public static List<FromItem> allFromItems(PlainSelect plainSelect) {
		// Extract all from items
		List<FromItem> fromItems = new ArrayList<FromItem>();
		fromItems.add(plainSelect.getFromItem());
		if (plainSelect.getJoins() != null) {
			for (Join join : plainSelect.getJoins()) {
				fromItems.add(join.getRightItem());
			}			
		}
		return fromItems;
	}

}
