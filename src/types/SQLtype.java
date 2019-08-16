package types;

/**
 * Represents the type of an SQL expression.
 * 
 * @author immanueltrummer
 *
 */
public enum SQLtype {
	BOOL, BYTE, INT, LONG, DOUBLE, 
	CHAR, STRING, 
	DATE, TIME, TIMESTAMP, 
	YM_INTERVAL, DT_INTERVAL,
	STRING_CODE,	// strings represented by integer codes (from dictionary)
	ANY_TYPE 		// placeholder used in situations in which the precise
					// type cannot be determined yet (e.g., NULL values)
}
