package jni;

import com.opencsv.CSVReader;
import config.LoadConfig;
import data.IntData;
import expressions.ExpressionInfo;
import expressions.compilation.EvaluatorType;
import expressions.compilation.ExpressionCompiler;
import expressions.compilation.UnaryBoolEval;
import operators.RowRange;
import query.ColumnRef;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static diskio.LoadCSV.lineCount;
import static operators.OperatorUtils.split;

public class JNITest {
    static {
        try {
            System.load("/Users/tracy/Documents/Research/skinnerdb/src/jni/JNIdll.jnilib");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
    public static void main(String[] args) throws Exception {
        // Open CSV file for reading
        CSVReader csvReader = new CSVReader(
                new FileReader("/Users/tracy/Documents/Research/skinnerdb/imdb/csv/title.csv"), ',');
        String[] inputFields;
        int rowCtr = 0;
        int maxRows = LoadConfig.MAXROWS;
        int cardinality = lineCount("/Users/tracy/Documents/Research/skinnerdb/imdb/csv/title.csv");
        IntData intData = new IntData(cardinality);
        while ((inputFields = csvReader.readNext()) != null && rowCtr < maxRows) {
            int colCtr = 4;
            String field = inputFields[colCtr];
            boolean isNull = field==null||
                    field.isEmpty()||
                    field.equals("NULL");
            intData.isNull.set(rowCtr, isNull);
            try {
                intData.data[rowCtr] = isNull?0:Integer.parseInt(field);
            } catch (Exception e) {
                System.err.println("Error parsing field " + field +
                        " in column " + colCtr + " of line " + rowCtr);
            }
            ++rowCtr;
            if (rowCtr % 100000 == 0) {
                System.out.println("Loaded " + rowCtr + " rows");
            }
        }
        long timer1 = System.currentTimeMillis();
        List<Integer> satisfiedRows = new ArrayList<>(cardinality);
        for (int row = 0; row < cardinality; row++) {
            if (intData.data[row] < 1990) {
                satisfiedRows.add(row);
            }
        }
        long timer2 = System.currentTimeMillis();
        System.out.println("Java time: " + (timer2 - timer1));
//        List<RowRange> batches = split(cardinality);
//        int nrBatches = batches.size();
//        List<Integer> result = new ArrayList<>(cardinality);
//        List<Integer>[] resultsArray = new ArrayList[nrBatches];
//        ExpressionInfo unaryPred = new ExpressionInfo();
//        UnaryBoolEval unaryBoolEval = compilePred(unaryPred, columnMapping);
//        int nrThreads = 4;
//        ExecutorService executorService = Executors.newFixedThreadPool(nrThreads);
//        List<Future<Integer>> futures = new ArrayList<>();
//        int batchSize = cardinality / nrThreads;
//        int remaining = cardinality - batchSize * nrThreads;
//        for (int bid = 0; bid < nrThreads; bid++) {
////            RowRange batch = batches.get(bid);
//            int first = batchSize * bid + Math.min(remaining, bid);
//            int end = first + batchSize + (bid < remaining ? 1 : 0);
//            List<Integer> subResult = new ArrayList<>(end - first + 1);
//            resultsArray[bid] = subResult;
//            futures.add(executorService.submit(() -> {
////                System.out.println("[Start] Batch " + first);
//                long batchStart = System.currentTimeMillis();
//                // Evaluate predicate for each table row
//                for (int row = first; row < end; ++row) {
//                    if (intData.data[row] < 1990) {
//                        subResult.add(row);
//                    }
//                }
////                System.out.println("[End] Batch " + first);
//                long batchEnd = System.currentTimeMillis();
//                if (batchEnd - batchStart > 5) {
//                    System.out.println("Batch " + first + ": " + (batchEnd - batchStart));
//                }
//                return subResult.size();
//            }));
//        }
//        int totalSize = 0;
//        for (int i = 0; i < nrThreads; i++) {
//            Future<Integer> batchCount = futures.get(i);
//            totalSize += batchCount.get();
//        }
//        System.out.println("New Size: " + totalSize);
//        long exeEnd = System.currentTimeMillis();
//        for (List<Integer> subResult: resultsArray) {
//            if (subResult != null)
//                result.addAll(subResult);
//        }
        long timer3 = System.currentTimeMillis();
//        System.out.println("Parallel Java time: " + (exeEnd - timer2) + "; Merge Time: " + (timer3 - exeEnd));
        int[] array = JNIExec.exec(intData.data);
        long timer4 = System.currentTimeMillis();
        System.out.println("C++ time: " + (timer4 - timer3) + " " + array[0]);
        csvReader.close();
//        executorService.shutdown();
    }

    /**
     * Compiles evaluator for unary predicate.
     *
     * @param unaryPred			predicate to compile
     * @param columnMapping		maps query to database columns
     * @return					compiled predicate evaluator
     * @throws Exception
     */
    static UnaryBoolEval compilePred(ExpressionInfo unaryPred,
                                     Map<ColumnRef, ColumnRef> columnMapping) throws Exception {
        ExpressionCompiler unaryCompiler = new ExpressionCompiler(
                unaryPred, columnMapping, null, null,
                EvaluatorType.UNARY_BOOLEAN);
        unaryPred.finalExpression.accept(unaryCompiler);
        return (UnaryBoolEval)unaryCompiler.getBoolEval();
    }
}
