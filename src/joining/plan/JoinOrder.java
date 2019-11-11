package joining.plan;

import java.util.Arrays;

/**
 * Represents a join order (as opposed to a
 * left deep query plan, the join order does
 * not yet fix the point at which predicates
 * are evaluated).
 * 
 * @author immanueltrummer
 *
 */
public class JoinOrder {
	/**
	 * Number of tables that are joined.
	 */
	public final int nrJoinedTables;
	/**
	 * Order in which tables are joined.
	 */
	public final int[] order;
	/**
	 * Initializes join order.
	 * 
	 * @param order	order in which to join tables
	 */
	public JoinOrder(int[] order) {
		this.nrJoinedTables = order.length;
		this.order = Arrays.copyOf(order, nrJoinedTables);
	}
	/**
	 * Produces a new join order that extends the
	 * current one by one more table.
	 * 
	 * @param newTable	new table to append
	 * @return			extended join order
	 * @throws Exception 
	 */
	public JoinOrder extend(int newTable) {
		int newNrTables = nrJoinedTables + 1;
		int[] newOrder = Arrays.copyOf(order, newNrTables);
		newOrder[newNrTables - 1] = newTable;
		return new JoinOrder(newOrder);
	}
	/**
	 * Two join orders are equal if they order tables equally.
	 */
	@Override
	public boolean equals(Object otherOrder) {
		if (otherOrder == this) {
			return true;
		}
		if (otherOrder == null || !(otherOrder instanceof JoinOrder)) {
			return false;
		}
		return Arrays.equals(((JoinOrder)otherOrder).order, order); 
	}
	/**
	 * Hash code is based on join order.
	 */
	@Override
	public int hashCode() {
		return Arrays.hashCode(order);
	}

	/**
	 * Mapping a prefix of tables to a integer.
	 *
	 * @param prefixLen		the length of prefix.
	 * @return				according integer that can represent a prefix.
	 */
	public int getPrefixKey(int prefixLen) {
		int len = order.length;
		int hash = 0;
		int card = 1;
		for (int i = 0; i < prefixLen; i++) {
			int table = order[i];
			hash += table * card;
			card *= len;
		}
		return hash;
	}
	/**
	 * Hash codes considering join order and split table.
	 *
	 * @param splitTable	the table to split.
	 * @return				hash code corresponding to a join order and split table.
	 */
	public int splitHashCode(int splitTable) {
		int len = order.length;
		int hash = 0;
		int card = 1;
		for (int table : order) {
			hash += table * card;
			card *= len;
			if (table == splitTable) {
				break;
			}
		}
		return hash;
	}
}