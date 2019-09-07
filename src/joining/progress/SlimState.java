package joining.progress;

/**
 * Captures processing state for a specific join order.
 * Each object is according to a table in a unique join order.
 *
 * @author Ziyun Wei
 *
 */
public class SlimState {
    /**
     * Last position index in join order
     * over whose tuples we have iterated.
     */
    public int lastIndex;
    /**
     * Last sharable progress in the table that
     * we have seen.
     */
    public int progress;
    /**
     * The round count when the latest progress is updated.
     */
    public long episode;
    /**
     * Initializes the slim state.
     *
     */
    public SlimState() {
        lastIndex = 0;
        progress = 0;
        episode = 0;
    }

    /**
     * Write the state after a episode to the progress tracker.
     *
     * @param state         the state after a episode.
     * @param newProgress   progress of the table.
     * @param newEpisode    the current episode.
     */
    public void updateProgress(State state, int newProgress, long newEpisode) {
//        int newIndex = state.lastIndex;
        if (progress != newProgress || state.episode > episode) {
            progress = newProgress;
            episode = newEpisode;
        }
        // update the episode of written state to the latest one.
        state.episode = episode;
    }


    /**
     * Considers another state achieved for a join order
     * sharing the same prefix in the table ordering.
     * Updates ("fast forwards") this state if the
     * other state is ahead.
     *
     * @param state	    evaluation state achieved for join order sharing prefix
     * @param table	    length of prefix shared with other join order
     * @return			true iff the other state is ahead
     *
     */
    public boolean restoreState(State state, int table) {
        // Fast forward is only executed if other state is more advanced
        if (state.episode <= episode) {
            state.tupleIndices[table] = progress;
            state.episode = episode;
            // restore join index
//            state.lastIndex = 0;
            return true;
        }
        return false;
    }
    @Override
    public String toString() {
        return "Progress: " + progress + "\tEpisode: " + episode + "\tIndex: " + lastIndex;
    }
}
