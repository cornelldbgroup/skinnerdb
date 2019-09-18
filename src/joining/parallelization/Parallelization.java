package joining.parallelization;

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

    public abstract void execute(Set<ResultTuple> resultList) throws Exception;

    /**
     * Write logs into local files.
     *
     * @param logs  A list of logs for multiple threads.
     * @param path  log directory path.
     */
    public void writeLogs(List<String>[] logs, String path) {
        List<Thread> threads = new ArrayList<>();
        int nrThreads = logs.length;
        for(int i = 0; i < nrThreads; i++) {
            List<String> value = logs[i];
            int finalI = i;
            Thread T1 = new Thread(() -> {
                try {
                    saveToLog(finalI, value, path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            T1.start();
            threads.add(T1);
        }
        for (int i = 0; i < nrThreads; i++) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void saveToLog(int tid, List<String> logs, String path) throws IOException {
        String PATH = "./logs/" + path;

        File directory = new File(PATH);
        if (!directory.exists()){
            directory.mkdirs();
        }
        FileWriter writer = new FileWriter("./logs/" + path + "/" + tid +".txt");

        logs.forEach(log -> {
            try {
                writer.write(log + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        writer.close();
    }
}
