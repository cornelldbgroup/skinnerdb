package joining.progress;
/**
 * Captures evaluation progress made for one specific table
 * for a given join order prefix.
 *
 * @author Ziyun Wei
 *
 */
public class SlimProgress {
    /**
     * Latest state reached by any join order sharing
     * a certain table prefix.
     */
    SlimState latestState;
    /**
     * Points to nodes describing progress for next table.
     */
    final SlimProgress[] childNodes;

    public SlimProgress(int nrTables) {
        childNodes = new SlimProgress[nrTables];
    }
}
