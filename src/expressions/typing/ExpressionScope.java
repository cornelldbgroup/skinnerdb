package expressions.typing;

/**
 * Represents the scope of an expression fragment
 * (i.e., whether it evaluates to one value per
 * tuple or per tuple group).
 * 
 * @author immanueltrummer
 *
 */
public enum ExpressionScope {
	PER_TUPLE, PER_GROUP
}
