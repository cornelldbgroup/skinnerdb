package joining.parallel.join;

public class ThreadResult {
    public final int[] result;
    public final int count;
    public ThreadResult(int[] result) {
        this.result = result;
        count = result[0];
    }
}