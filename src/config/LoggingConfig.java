package config;

/**
 * Configures debugging output for different stages of
 * query analysis and query processing.
 * 
 * @author immanueltrummer
 *
 */
public class LoggingConfig {
	/**
	 * Whether to log sub-queries generated during unnesting.
	 */
	public final static boolean UNNESTING_VERBOSE = true;
	/**
	 * Whether to log intermediate steps during query analysis.
	 */
	public final static boolean QUERY_ANALYSIS_VERBOSE = false;
	/**
	 * Whether to log expressions after each transformation stage.
	 */
	public final static boolean EXPRESSIONS_VERBOSE = false;
	/**
	 * Whether to generate logging output while compiling expressions.
	 */
	public final static boolean COMPILATION_VERBOSE = false;
	/**
	 * Whether to print per-row evaluation results for expressions.
	 */
	public final static boolean EVALUATION_ROW_VERBOSE = false;
	/**
	 * Whether to generate verbose output on buffer activity.
	 */
	public final static boolean BUFFER_VERBOSE = false;
	/**
	 * Whether to generate verbose output about pre-processing.
	 */
	public final static boolean PREPROCESSING_VERBOSE = false;
	/**
	 * Whether to generate verbose output during index creation.
	 */
	public final static boolean INDEXING_VERBOSE = false;
	/**
	 * Whether to print out intermediate results after each
	 * processing step - this is only practical for very small
	 * database for debugging purposes.
	 */
	public final static boolean PRINT_INTERMEDIATES = false;
	/**
	 * Whether to generate logging output when generating
	 * join index wrappers.
	 */
	public final static boolean INDEX_WRAPPER_VERBOSE = false;
	/**
	 * How many log entries to generate during join processing
	 * per query - set to zero to avoid join logging.
	 */
	public final static int MAX_JOIN_LOGS = 0;
	/**
	 * Whether to generate debugging output during post-processing.
	 */
	public final static boolean POST_PROCESSING_VERBOSE = false;
	/**
	 * Whether to generate debugging output during joining.parallel join.
	 */
	public final static boolean PARALLEL_JOIN_VERBOSE = false;
	/**
	 * Whether to generate performance output during.
	 */
	public final static boolean PERFORMANCE_VERBOSE = false;
}
