package preprocessing;

import buffer.BufferManager;
import catalog.info.ColumnInfo;
import config.GeneralConfig;
import config.ParallelConfig;
import indexing.Index;
import joining.parallel.indexing.PartitionIndex;
import query.ColumnRef;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RecursiveAction;

public class PreprocessTask extends RecursiveAction {
    public final QueryInfo query;
    public final Context preSummary;
    public final Set<ColumnRef> requiredCols;

    public PreprocessTask(QueryInfo query, Context preSummary, Set<ColumnRef> requiredCols) {
        this.query = query;
        this.preSummary = preSummary;
        this.requiredCols = requiredCols;
    }


    @Override
    protected void compute() {
        invokeAll(createSubtasks());
    }

    private List<TableTask> createSubtasks() {
        List<TableTask> subtasks = new ArrayList<>();
        for (String alias: query.aliasToTable.keySet()) {
            // Collect required columns (for joins and post-processing) for this table
            List<ColumnRef> curRequiredCols = new ArrayList<>();
            for (ColumnRef requiredCol : requiredCols) {
                if (requiredCol.aliasName.equals(alias)) {
                    curRequiredCols.add(requiredCol);
                }
            }
            TableTask tableTask = new TableTask(query, preSummary, alias, curRequiredCols);
            subtasks.add(tableTask);
        }
        return subtasks;
    }

}
