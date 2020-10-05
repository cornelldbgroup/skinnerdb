package joining.parallel.parallelization.task;

import joining.result.ResultTuple;

import java.util.Collection;
import java.util.List;

/**
 * Execution results for search parallelization
 *
 * @author Anonymous
 */
public class TaskResult {
    /**
     * The list of results collected by a thread.
     */
    public Collection<ResultTuple> result;
    /**
     * A list of log sentences.
     */
    public List<String> logs;
    /**
     * The id of a thread.
     */
    public int id;

    public TaskResult(Collection<ResultTuple> result, List<String> logs, int id) {
        this.result = result;
        this.logs = logs;
        this.id = id;
    }
}
