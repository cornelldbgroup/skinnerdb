package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.JoinConfig;
import config.NamingConfig;
import diskio.PathUtil;
import joining.JoinProcessor;
import joining.uct.TreeSearchPolicy;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.QueryInfo;
import statistics.JoinStats;
import statistics.PostStats;
import statistics.PreStats;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Test the performance of all those three RL search algorithm, UCT, BRUE and BRUEI
 */
public class SkinnerPerformanceBench {

    /**
     * the test number
     */
    private final static int testNr = 3;

//    private final static int[] testLearningBudgets = {500};

//    private final static double[] scales = {1E-15, 1E-11, 1E-7, 1E-5, 1E-4, 1E-3, 1, 1E3};
//
//    private final static String[] scaleStr = {"-15", "-11", "-7", "-5", "-4", "-3", "1", "3"};

    private final static double[] scales = {1E-15, 1};

    private final static String[] scaleStr = {"-15", "1"};

//    private final static int[] testExecutionBudgets = {9000, 7000, 5000, 3000, 1000, 800, 600, 400, 200};

//    private final static int[] samplePerLearns = {10, 20, 40, 60, 80, 100};

    private final static TreeSearchPolicy[] policies = {TreeSearchPolicy.UCT, TreeSearchPolicy.BRUE, TreeSearchPolicy.BRUEI};

    private final static String[] policyStr = {"UCT", "BRUE", "BRUEI"};

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

        for (int p = 1; p < 3; p++) {
            int j = 0;
            JoinConfig.TREE_POLICY = policies[p];
            for (double scale : scales) {
                JoinConfig.REWARD_SCALE = scale;
//              JoinConfig.LEARN_BUDGET_EPISODE = 500;
//              JoinConfig.EXECUTION_BUDGET_EPISODE = executionBudget;
//              JoinConfig.SAMPLE_PER_LEARN = 100;

//              PrintWriter writer = new PrintWriter(new File("result.txt"));
//              PrintWriter writer = new PrintWriter(new File(String.format("lb%d-eb%d-nr%d.txt", learningBudget, executionBudget, samplePerLearn)));
//              PrintWriter writer = new PrintWriter(new File(String.format("lb%d.txt", learningBudget)));
//              PrintWriter writer = new PrintWriter(new File(String.format("lb%d-eb%d-nr100.txt", learningBudget, executionBudget)));

                for (int i = 0; i < testNr; i++) {
                    PrintWriter writer = new PrintWriter(new File(String.format("%s_s%s_%d.txt", policyStr[p], scaleStr[j], i)));
                    for (Map.Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
                        double sumPre = 0;
                        double sumJoin = 0;
                        double sumPost = 0;
                        double sumTotal = 0;

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
                        writer.print(entry.getKey() + "\t");
                        writer.print(sumTotal + "\t");
                        writer.print(PreStats.preMillis + "\t");
                        writer.print(JoinStats.joinMillis + "\t");
                        writer.print(PostStats.postMillis + "\t");
                        writer.print(PreStats.filterProjectMillis + "\t");
                        writer.print(JoinStats.pureJoinMillis + "\t");
                        writer.print(JoinStats.nrTuples + "\t");
                        writer.print(JoinStats.nrFastBacktracks + "\t");
                        writer.print(JoinStats.nrIterations + "\t");
                        writer.print(JoinStats.nrIndexLookups + "\t");
                        writer.print(JoinStats.nrIndexEntries + "\t");
                        writer.print(JoinStats.nrUniqueIndexLookups + "\t");
                        writer.print(JoinStats.nrUctNodes + "\t");
                        writer.print(JoinStats.nrPlansTried + "\t");
                        writer.print(JoinStats.skinnerJoinCard + "\t");
                        writer.print(JoinStats.nrSamples + "\t");
                        writer.print(JoinStats.avgReward + "\t");
                        writer.print(JoinStats.maxReward + "\t");
                        writer.println(JoinStats.totalWork);
                        writer.flush();
                    }
//                        writer.println("===============" + entry.getKey() + "==========================");
//                        writer.println("Pre:" + sumPre / testNr + "ms");
//                        writer.println("Join:" + sumJoin / testNr + "ms");
//                        writer.println("Post:" + sumPost / testNr + "ms");
//                        writer.println("Total time:" + sumTotal / testNr + "ms");
                    writer.close();
                }
                j++;
            }
        }
//            }
//        }
    }
}