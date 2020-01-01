package joining.parallel.parallelization;

import joining.result.ResultTuple;
import preprocessing.Context;
import query.QueryInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class Parallelization {
    /**
     * Number of threads
     */
    public int nrThreads;
    /**
     * executing query
     */
    public QueryInfo query;
    /**
     * query execution context
     */
    public Context context;



    /**
     * initialization of parallelization
     * @param nrThreads         the number of threads
     * @param query             select query with join predicates
     * @param context           query execution context
     */
    public Parallelization(int nrThreads, int budget, QueryInfo query, Context context) {
        this.nrThreads = nrThreads;
        this.query = query;
        this.context = context;
    }

    /**
     * Execute Join phase using parallelization method.
     *
     * @param resultList        a list of result tuples
     * @throws Exception
     */
    public abstract void execute(Set<ResultTuple> resultList) throws Exception;
}
