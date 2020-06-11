#Extensions over the master branch
## benchmark
- Supporting a graceful format of benchmark statistics for each sub-query. E.g. statistics for PreMillis is [subMillis<sub>1</sub>,...,subMillis<sub>n</sub>].
- See optsequential branch.
## buffer
- Collecting the size of indexes and according data.
## config
- ParallelConfig: batch sizes, thread numbers, etc
## logs
- Recording the behaviour for each thread.
## indexing
- TemporaryIndex that is created during query processing.
- Reusing the keyToPositions map to avoid heavy grouping process.
- Adding an byte array to inform the current row belongs to which thread.
- Parallel index creation.
- Make sure: whether avoiding interpretation can work for multi-thread.
## joining
- DPJoin for data parallel. Specifically, we add ModJoin to use module method to partition data.
- In the wrapper, we need to check whether the current table is the split table during the evaluation and jumping functions.
- In the progress tracker, we need to modify the structure for each state saved in the node. Also it is required to extend the progress saved for each thread.
- separated UCT tree and progress tracker.
- proposedSplitTable for each thread.
- EndPlan -> SplitCoordinator.
- SplitCoordinator maintains split statistics.
- threads are doing individual selection
- In the UCT tree, we need to extend statistics structure for each thread. 
- In the thread task, we need to add a coordinator to check and optimize the split table for the last join plan.
- Reduced multi-level calls.
- Rename to joinCoordinator and joinWorker.
- joining.directory.seq/parallel.
## postprocessing
- Supporting AVG function by adding AvgAggregate as an operator.
- Supporting parallel mapping.
- Supporting parallel aggregation without groupy clauses.


