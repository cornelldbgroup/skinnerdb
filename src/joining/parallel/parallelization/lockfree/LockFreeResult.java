package joining.parallel.parallelization.lockfree;

import joining.result.ResultTuple;

import java.util.Collection;
import java.util.List;

public class LockFreeResult {
    public Collection<ResultTuple> result;
    public List<String> logs;
    public int id;

    public LockFreeResult(Collection<ResultTuple> result, List<String> logs, int id) {
        this.result = result;
        this.logs = logs;
        this.id = id;
    }
}
