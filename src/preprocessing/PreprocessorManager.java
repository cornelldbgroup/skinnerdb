package preprocessing;

public class PreprocessorManager {
    private static final Preprocessor INSTANCE;

    static {
        INSTANCE = new SearchPreprocessor();
    }

    public static Preprocessor getPreprocessor() {
        return INSTANCE;
    }
}
