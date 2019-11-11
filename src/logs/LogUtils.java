package logs;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utils of writing logs.
 *
 * @author Ziyun Wei
 */
public class LogUtils {
    /**
     * Write logs into local files.
     *
     * @param logs  A list of logs for multiple threads.
     * @param path  log directory path.
     */
    public static void writeLogs(List<String>[] logs, String path) {
        List<Thread> threads = new ArrayList<>();
        int nrThreads = logs.length;
        String PATH = "./logs/" + path;
        File directory = new File(PATH);
        if (!directory.exists()){
            directory.mkdirs();
        }
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

    private static void saveToLog(int tid, List<String> logs, String path) throws IOException {
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
