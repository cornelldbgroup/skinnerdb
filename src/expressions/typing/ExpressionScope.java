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
	PER_TUPLE,	// expression yields one value per input tuple 
	PER_GROUP, 	// expression yields one value per tuple group
	ANY_SCOPE	// scope cannot be determined yet (e.g., for constants)
}
