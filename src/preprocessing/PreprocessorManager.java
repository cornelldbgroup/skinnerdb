package preprocessing;

public class PreprocessorManager {
    private static final Preprocessor INSTANCE;

    static {
        INSTANCE = new OldPreprocessor();
    }

    public static Preprocessor getPreprocessor() {
        return INSTANCE;
    }
}
