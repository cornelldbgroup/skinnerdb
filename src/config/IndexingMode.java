package config;

/**
 * Describes criteria according to which columns
 * are selected for indexing at startup time.
 * 
 * @author immanueltrummer
 *
 */
public enum IndexingMode {
	ALL,		// create indices on all columns
	ONLY_KEYS	// create indices only on key columns
}
