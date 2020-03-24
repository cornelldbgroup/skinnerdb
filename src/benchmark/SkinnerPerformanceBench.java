package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.JoinConfig;
import config.NamingConfig;
import diskio.PathUtil;
import joining.JoinProcessor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.QueryInfo;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map;

public class SkinnerPerformanceBench {

    private final static int testNr = 1;

    //private final static int[] testLearningBudgets = {20, 40, 60, 80, 100, 120, 140, 160, 180, 200};

    //private final static int[] testExecutionBudgets = {5000, 3000, 1000, 800, 600, 400, 200};
    //{5000, 4000, 3000, 2000, 1000, 800, 600, 400, 200};

    //private final static int[] samplePerLearns = {10, 20, 40, 60, 80, 100};

    public static void main(String[] args) throws Exception {
        // Check for command line parameters
        if (args.length != 2) {
            System.out.println("Specify DB dir and query directory!");
            return;
        }

        // Initialize database
        String dbDir = args[0];
        PathUtil.initSchemaPaths(dbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        System.out.println("Loading data ...");
        GeneralConfig.inMemory = true;
        BufferManager.loadDB();
        System.out.println("Data loaded.");

        Map<String, PlainSelect> nameToQuery = BenchUtil.readAllQueries(args[1]);
//        for (int samplePerLearn : samplePerLearns) {
//            for (int executionBudget: testExecutionBudgets) {
//                for (int learningBudget : testLearningBudgets) {
//                    JoinConfig.BUDGET_LEARNING = learningBudget;
//                    JoinConfig.BUDGET_EXECUTION = executionBudget;
//                    JoinConfig.SAMPLE_PER_LEARN = samplePerLearn;

        PrintWriter writer = new PrintWriter(new File("result.txt"));
//                    PrintWriter writer = new PrintWriter(new File(String.format("lb%d-eb%d-nr%d.txt", learningBudget, executionBudget, samplePerLearn)));
        for (Map.Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
            double sumPre = 0;
            double sumJoin = 0;
            double sumPost = 0;
            double sumTotal = 0;
            for (int i = 0; i < testNr; i++) {
                System.out.println(entry.getKey());
                System.out.println(entry.getValue().toString());
                // Run queries
                long startMillis = System.currentTimeMillis();
                QueryInfo query = new QueryInfo(entry.getValue(),
                        false, -1, -1, null);
                Context preSummary = Preprocessor.process(query);
                long joinStartMillis = System.currentTimeMillis();
                JoinProcessor.process(query, preSummary);
                long postStartMillis = System.currentTimeMillis();
                PostProcessor.process(query, preSummary,
                        NamingConfig.FINAL_RESULT_NAME, true);
                long postDuration = System.currentTimeMillis() - postStartMillis;
                long joinDuration = postStartMillis - joinStartMillis;
                long preDuration = joinStartMillis - startMillis;
                long totalMillis = System.currentTimeMillis() - startMillis;
                sumPre += preDuration;
                sumJoin += joinDuration;
                sumPost += postDuration;
                sumTotal += totalMillis;
                // Clean up
                BufferManager.unloadTempData();
                CatalogManager.removeTempTables();

            }
            writer.println("===============" + entry.getKey() + "==========================");
            writer.println("Pre:" + sumPre / testNr + "ms");
            writer.println("Join:" + sumJoin / testNr + "ms");
            writer.println("Post:" + sumPost / testNr + "ms");
            writer.println("Total time:" + sumTotal / testNr + "ms");
        }
        writer.close();
//                }
//            }
//        }
    }
}