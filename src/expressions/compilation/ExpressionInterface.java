package expressions.compilation;

import java.util.Arrays;

/**
 * Wrapper class containing input and output to
 * compiled expressions.
 * 
 * @author immanueltrummer
 *
 */
public class ExpressionInterface {
	/**
	 * Contains the row index for cases in which one table is considered.
	 */
	public static int unaryIndex;
	/**
	 * Contains for each alias the index of the current focus tuple.
	 */
	public static int[] tupleIndices;
	/**
	 * 1 iff the result of evaluating the last expression was null.
	 */
	public static byte nullResult;
	/**
	 * Contains result of evaluating Boolean expression.
	 * A value greater than zero means true.
	 */
	public static byte boolResult;
	/**
	 * Contains result of evaluating byte expression.
	 */
	public static byte byteResult;
	/**
	 * Contains result of evaluating integer expression.
	 */
	public static int intResult;
	/**
	 * Contains result of evaluating long expression.
	 */
	public static long longResult;
	/**
	 * Contains result of evaluating character expression.
	 */
	public static char charResult;
	/**
	 * Contains result of evaluating string expression.
	 */
	public static String stringResult;
	/**
	 * Print values of all interface fields.
	 */
	public static void print() {
		System.out.println("ExpressionInterface fields:");
		System.out.println("unaryIndex:\t" + unaryIndex);
		System.out.println("tupleIndices:\t" + Arrays.toString(tupleIndices));
		System.out.println("nullResult:\t" + nullResult);
		System.out.println("boolResult:\t" + boolResult);
		System.out.println("byteResult:\t" + byteResult);
		System.out.println("intResult:\t" + intResult);
		System.out.println("longResult:\t" + longResult);
		System.out.println("charResult:\t" + charResult);
		System.out.println("stringResult:\t" + stringResult);
	}
}
