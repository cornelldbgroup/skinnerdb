package joining.parallel.progress;

import joining.progress.State;

public class TaskNode {
    /**
     * Latest state reached by any join order sharing
     * a certain table prefix.
     */
    TaskState latestState;
    /**
     * Points to nodes describing tasks for next table.
     */
    final TaskNode[] childNodes;

    public TaskNode(int nrTables) {
        childNodes = new TaskNode[nrTables];
    }
}
