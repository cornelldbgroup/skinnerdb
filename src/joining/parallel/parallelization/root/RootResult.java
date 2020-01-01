package joining.parallel.parallelization.root;

import joining.result.ResultTuple;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Execution results for root parallelization
 *
 * @author Ziyun Wei
 */
public class RootResult {
    /**
     * The list of results collected by a thread.
     */
    public Set<ResultTuple> result;
    /**
     * A list of log sentences.
     */
    public List<String> logs;
    /**
     * The id of a thread.
     */
    public int id;

    public RootResult(Set<ResultTuple> result, List<String> logs, int id) {
        this.result = result;
        this.logs = logs;
        this.id = id;
    }
}
