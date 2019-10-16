package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import config.NamingConfig;
import diskio.PathUtil;
import joining.JoinProcessor;
import joining.JoinProcessorNew;
import multiquery.GlobalContext;
import net.sf.jsqlparser.statement.select.PlainSelect;
import postprocessing.PostProcessor;
import preprocessing.Context;
import preprocessing.Preprocessor;
import print.RelationPrinter;
import query.QueryInfo;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Map;

public class MultiQueryBenchmark {

    public static void main(String[] args) throws Exception {

        PrintStream out = new PrintStream(new FileOutputStream("new5.txt"));
        System.setOut(out);

        String dbDir = //"/home/gid-wangj3/skinnerDB/data/imdb/";
                "/home/jw2544/imdbm/";
                //"/home/gid-wangj3/skinnerDB/data/imdb/";
        //"/home/jw2544/imdbl/";
        //"/home/jw2544/dataset/skinnerimdb/";//"/home/jw2544/imdbl/";
        PathUtil.initSchemaPaths(dbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        //System.out.println("Loading data ...");
        GeneralConfig.inMemory = true;
        BufferManager.loadDB();
        //System.out.println("Data loaded.");
        //Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
        String queryDir = //"/home/gid-wangj3/multi-query/imdb/queries/";
                "/home/jw2544/Documents/multi-query/imdb/queries/";
                //"/home/gid-wangj3/multi-query/imdb/queries/";
                //"/home/jw2544/Documents/multi-query/imdb/queries/";
        Map<String, PlainSelect> nameToQuery = BenchUtil.readAllQueries(queryDir);
        int nrQueries = nameToQuery.size();
        QueryInfo[] queries = new QueryInfo[nrQueries];
        Context[] preSummaries = new Context[nrQueries];
        int queryNum = 0;
        long startMillis = System.currentTimeMillis();
        for (Map.Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
            QueryInfo query = new QueryInfo(queryNum, entry.getValue(),
                    false, -1, -1, null);
            queries[queryNum] = query;
            preSummaries[queryNum] = Preprocessor.process(query);
            queryNum++;
        }
        GlobalContext.initCommonJoin(queries);
        JoinProcessorNew.process(queries, preSummaries);
        for(int i = 0; i < queryNum; i++) {
            PostProcessor.process(queries[i], preSummaries[i]);
            String resultRel = NamingConfig.FINAL_RESULT_NAME;
            RelationPrinter.print(resultRel);
        }
        long totalMillis = System.currentTimeMillis() - startMillis;
        System.out.println("Total time:" + totalMillis + "ms");
        /*
        while(GlobalContext.firstUnfinishedNum > 0) {
            //we don't enable preprocessing, preprocessing is also on the UCT search
            int triedQuery = GlobalContext.firstUnfinishedNum;
            JoinProcessor.process(queries.get(triedQuery), preSummaries.get(triedQuery));

            if(queries)
            GlobalContext.aheadFirstUnfinish();
        }
        */

        //may be apply post-processing
    }

}
