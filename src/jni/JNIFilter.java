package jni;


public class JNIFilter {
    public static native int filter(String[] ast, int[][] intData,
                                      double[][] doubleData,
                                      int[] indexRows, int table_size,
                                      int[][] intSrcData, double[][] doubleSrcData,
                                      int[][] intTargetData, double[][] doubleTargetData,
                                      int nr_threads);
    public static native int[] sort(int[] filtered_rows, int start, int end);
    public static native void fill(String[] jni_dictionary);
    public static native void materialize(int[][] intSrcData, double[][] doubleSrcData,
                                          int[][] intTargetData, double[][] doubleTargetData,
                                          int[] rows, int nr_threads, boolean needSort);
    public static native int groupby(int[][] intSrcData, double[][] doubleSrcData,
                                          int[] intTargetData, int cardinality, int nr_threads);
    public static native int[] partialSort(int[] int_data, int[] sorted_rows, int cardinality,
                                    int nr_threads);
    public static native void postprocessing(int[][] intSrcData, double[][] doubleSrcData,
                                             int[] intGroupData, int[] doubleGroupData,
                                             String[] intGroupMap, String[] doubleGroupMap,
                                             int groupIdx, int maxKeys,
                                             String[][] astNodes, boolean[] intCols,
                                             int nrIntSelects, String havingNode,
                                             boolean needSort,
                                             int[][] intTargetData, double[][] doubleTargetData,
                                             int cardinality, int nr_threads);
}
