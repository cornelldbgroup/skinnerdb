package joining.parallel.parallelization.test;

import joining.parallel.plan.LeftDeepPartitionPlan;

public class RoundTrace {
    public final int roundCtr;
    public final int[] startProgress;
    public final int[] offsets;
    public final int[] optimalJoinOrder;
    public final int[] joinOrder;
    public final int splitTable;
    public final LeftDeepPartitionPlan plan;

    public RoundTrace(int roundCtr, int[] startProgress, int[] offsets,
                      int[] optimalJoinOrder, int[] joinOrder,
                      int splitTable, LeftDeepPartitionPlan plan) {
        this.roundCtr = roundCtr;
        this.startProgress = startProgress.clone();
        this.offsets = offsets.clone();
        this.splitTable = splitTable;
        this.optimalJoinOrder = optimalJoinOrder;
        this.joinOrder = joinOrder;
        this.plan = plan;
    }
}
