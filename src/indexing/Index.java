package indexing;

/**
 * Common superclass of all indexing structures.
 * 
 * @author immanueltrummer
 *
 */
public abstract class Index {
    /**
     * Writes data to disk at specified path.
     *
     * @param positions	store data
     * @throws Exception
     */
    public abstract void store(int[] positions) throws Exception;

    public abstract void clear();
}
