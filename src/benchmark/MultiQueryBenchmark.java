package benchmark;

import buffer.BufferManager;
import catalog.CatalogManager;
import config.GeneralConfig;
import diskio.PathUtil;
import joining.JoinProcessor;
import multiquery.GlobalContext;
import net.sf.jsqlparser.statement.select.PlainSelect;
import preprocessing.Context;
import preprocessing.Preprocessor;
import query.QueryInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiQueryBenchmark {

    public static void main(String[] args) throws Exception {
        String dbDir = "/home/jw2544/imdbl/";
                //"/home/jw2544/dataset/skinnerimdb/";//"/home/jw2544/imdbl/";
        PathUtil.initSchemaPaths(dbDir);
        CatalogManager.loadDB(PathUtil.schemaPath);
        PathUtil.initDataPaths(CatalogManager.currentDB);
        System.out.println("Loading data ...");
        GeneralConfig.inMemory = true;
        BufferManager.loadDB();
        //System.out.println("Data loaded.");
        //Indexer.indexAll(StartupConfig.INDEX_CRITERIA);
        String queryDir = "/home/jw2544/Documents/multi-query/imdb/queries/";
        Map<String, PlainSelect> nameToQuery = BenchUtil.readAllQueries(queryDir);
        int nrQueries = nameToQuery.size();
        QueryInfo[] queries = new QueryInfo[nrQueries];
        Context[] preSummaries = new Context[nrQueries];
        int queryNum = 0;
        for (Map.Entry<String, PlainSelect> entry : nameToQuery.entrySet()) {
            QueryInfo query = new QueryInfo(queryNum, entry.getValue(),
                    false, -1, -1, null);
            queries[queryNum] = query;
            preSummaries[queryNum] = Preprocessor.process(query);
            queryNum++;
        }
        GlobalContext.initCommonJoin(queries);
        JoinProcessor.process(queries, preSummaries);
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
