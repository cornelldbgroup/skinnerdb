package preprocessing;

public class PreprocessorManager {
    private static final Preprocessor INSTANCE;

    static {
        INSTANCE = new BasicPreprocessor();
    }

    public static Preprocessor getPreprocessor() {
        return INSTANCE;
    }
}
