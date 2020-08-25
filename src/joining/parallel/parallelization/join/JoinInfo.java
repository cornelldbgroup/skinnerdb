package joining.parallel.parallelization.join;

import joining.parallel.plan.LeftDeepPartitionPlan;

import java.util.concurrent.ConcurrentSkipListSet;

public class JoinInfo {
    public final LeftDeepPartitionPlan plan;
    public final ConcurrentSkipListSet<JoinMicroTask> tasks;
    public final int[] tupleIndices;
    public JoinInfo(LeftDeepPartitionPlan plan,
                    ConcurrentSkipListSet<JoinMicroTask> tasks,
                    int[] tupleIndices) {
        this.plan = plan;
        this.tasks = tasks;
        this.tupleIndices = tupleIndices;
    }
}
