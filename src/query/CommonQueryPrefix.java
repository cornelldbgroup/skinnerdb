package query;

import java.util.ArrayList;

public class CommonQueryPrefix {

    public int prefixLen;

    public int[] joinOrder;

    public int reusedQuery;

    public CommonQueryPrefix(int prefixLen, int[] joinOrder, int reusedQuery) {
        this.prefixLen = prefixLen;
        this.joinOrder = joinOrder;
        this.reusedQuery = reusedQuery;
    }
}
