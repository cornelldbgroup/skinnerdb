package preprocessing;

import query.QueryInfo;

public interface Preprocessor {
    Context process(QueryInfo query) throws Exception;
}
