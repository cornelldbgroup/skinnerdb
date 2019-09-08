package query;

public class CommonQueryPrefix {

    public int prefixLen;

    public int[] joinOrder;

    public int shift;

    public CommonQueryPrefix(int prefixLen, int[] joinOrder, int shift) {
        this.prefixLen = prefixLen;
        this.joinOrder = joinOrder;
        this.shift = shift;
    }
}
