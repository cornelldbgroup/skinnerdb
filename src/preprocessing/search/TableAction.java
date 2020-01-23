package preprocessing.search;

import search.Action;

public class TableAction implements Action {
    public final int table;

    public TableAction(int table) {
        this.table = table;
    }
}
