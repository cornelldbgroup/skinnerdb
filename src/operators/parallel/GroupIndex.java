package operators.parallel;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a group of rows sharing the
 * same value in a set of columns of the
 * same cardinality.
 *
 * @author Ziyun Wei
 *
 */
public class GroupIndex {
    /**
     * The identification of each group.
     */
    public int gid;
    /**
     * The list of rows that belong to a certain group.
     */
    public final List<Integer> rows;
    /**
     * The number of all not null elements.
     */
    public int number;

    /**
     * Initializes this group for a given row as
     * representative, using given set of columns
     * for calculating groups.
     *
     */
    public GroupIndex(int gid, int size) {
        this.gid = gid;
        if (size < 0) {
            this.rows = new ArrayList<>();
        }
        else {
            this.rows = new ArrayList<>(size);
        }
    }

    public void addRow(int rowCtr) {
        rows.add(rowCtr);
        number++;

    }

    public int getRow() {
        return rows.get(0);
    }

    public int getCard() {
        return rows.size();
    }

    public void merge(GroupIndex curIndex) {
        rows.addAll(curIndex.rows);
        number += curIndex.number;
    }
}
