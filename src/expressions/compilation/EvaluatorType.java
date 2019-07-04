package expressions.compilation;

/**
 * Describes type of expression evaluator to generate.
 * 
 * @author immanueltrummer
 *
 */
public enum EvaluatorType {
	UNARY_GENERIC,	// unary input/output via static variables in ExpressionInterface
	KARY_GENERIC,
	UNARY_BOOLEAN,	// input is single row number, output is byte
	KARY_BOOLEAN,	// input is tuple index vector, output is byte
	UNARY_INT,		// input is single row number and array, 
					// array is filled with integer result, returns not-null flag
	UNARY_LONG,		// input is single row number and array,
					// array is filled with long result, returns not-null flag
	UNARY_DOUBLE,	// input is single row number and array,
					// array is filled with double result, returns not-null flag
	UNARY_STRING,	// input is single row number and array,
					// array is filled with string result, returns not-null flag
}
