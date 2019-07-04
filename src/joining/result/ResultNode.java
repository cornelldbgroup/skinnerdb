package joining.result;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents one node in a tree representation
 * of a join result.
 * 
 * @author immanueltrummer
 *
 */
public class ResultNode {
	/**
	 * Maps tuple indices to child nodes.
	 */
	Map<Integer, ResultNode> childNodes = 
			new HashMap<Integer, ResultNode>();
}
