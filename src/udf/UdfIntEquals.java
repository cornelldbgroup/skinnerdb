package udf;

/**
 * UDF predicate returning 1 if two integers are equal
 * (0 otherwise).
 * 
 * @author immanueltrummer
 *
 */
public class UdfIntEquals {
	/**
	 * Compare two integers and return 1
	 * if they are equal, 0 otherwise.
	 * 
	 * @param x	first integer
	 * @param y	second integer
	 * @return	1 iff both are equal
	 */
	public static int evaluate(int x, int y) {
		return x==y?1:0;
	}
}
