package benchmark;

import config.JoinConfig;
import org.apache.commons.math3.distribution.NormalDistribution;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParallelMethods {

    public static void main(String[] args) throws Exception {

        int[] joins = new int[]{10, 20, 30, 40, 50};
        double[] orderSds = new double[]{Double.MIN_VALUE, 1E-5, 1E-1, 1, 10};
        int[] threads = new int[]{2, 4, 6, 8, 10};
        PrintWriter benchOut = new PrintWriter("./artificial.txt");
        writeBenchHeader(benchOut);
        for (int tid = 0; tid < threads.length; tid++) {
            for (int jid = 0; jid < joins.length; jid++) {
                for (int sid = 0; sid < orderSds.length; sid ++) {
                    int nrThreads = threads[tid];
                    int nrJoined = joins[jid];
                    // we assume if the first table is the same,
                    // the rewards follows a normal distribution.
                    int orderMean = 0;
                    double orderSd = orderSds[sid];
                    double[] armSampling = new double[nrJoined];
                    NormalDistribution armRewardsDist = new NormalDistribution(orderMean, orderSd);
                    for (int i = 0; i < nrJoined; i++) {
                        armSampling[i] = armRewardsDist.sample();
                    }

                    double minOrder = 5 - Arrays.stream(armSampling).min().getAsDouble();

                    for (int i = 0; i < nrJoined; i++) {
                        armSampling[i] = Math.pow(10, -1 * (minOrder + armSampling[i]));
                    }

                    System.out.println("Testing: " + nrThreads + "\t" + nrJoined + "\t" + orderSd);

                    int dataCount = runDataParallel(nrThreads, nrJoined, armSampling);
                    writeStats("data", nrThreads, nrJoined, orderSd, dataCount, benchOut);
                    int searchCount = runSearchParallel(nrThreads, nrJoined, armSampling);
                    writeStats("search", nrThreads, nrJoined, orderSd, searchCount, benchOut);
                }
            }
        }
        benchOut.close();
    }

    public static int runSearchParallel(int nrThreads, int nrJoined, double[] armSampling) {
        System.out.println("Search Parallel");
        double[][] dataOffsets = new double[nrThreads][nrJoined];
        BigDecimal[][] accumulatedReward = new BigDecimal[nrThreads][nrJoined];
        int[][] nrTries = new int[nrThreads][nrJoined];

        ExecutorService executorService = Executors.newFixedThreadPool(nrThreads);
        List<Future<Integer>> futures = new ArrayList<>();
        AtomicBoolean isFinished = new AtomicBoolean(false);

        ReadWriteLock[] readWriteLocks = new ReadWriteLock[nrThreads];
        for (int i = 0; i < nrThreads; i++) {
            readWriteLocks[i] = new ReentrantReadWriteLock();
            for (int j = 0; j < nrJoined; j++) {
                accumulatedReward[i][j] = new BigDecimal(0);
            }
        }
        for (int i = 0; i < nrThreads; i++) {
            int finalI = i;
            futures.add(executorService.submit(() -> {
                int count = 0;
                while (!isFinished.get()) {
                    count++;
                    int nrVisits = 0;
                    int[] tries = new int[nrJoined];
                    BigDecimal[] rewards = new BigDecimal[nrJoined];
                    for (int recAction = 0; recAction < nrJoined; recAction++) {
                        rewards[recAction] = new BigDecimal(0);
                    }
                    // collect all statistics
                    for (int tid = 0; tid < nrThreads; tid++) {
                        readWriteLocks[tid].readLock().lock();
                        for (int recAction = 0; recAction < nrJoined; recAction++) {
                            int threadTries = nrTries[tid][recAction];
                            tries[recAction] += threadTries;
                            nrVisits += threadTries;
                            rewards[recAction] = rewards[recAction].add(accumulatedReward[tid][recAction]);
                        }
                        readWriteLocks[tid].readLock().unlock();
                    }
                    List<Integer> actions = IntStream.range(0, nrJoined).boxed().collect(Collectors.toList());
                    actions.forEach(action -> {
                        int nrTry = tries[action];
                        rewards[action] = nrTry == 0 ? new BigDecimal(0) : rewards[action].divide(new BigDecimal(nrTry), 0);
                    });
                    actions = actions.stream().sorted(
                            Comparator.comparing(action ->  -1 * rewards[action].doubleValue())).collect(Collectors.toList());

                    int bestAction = -1;
                    double bestQuality = -1;
                    if (finalI == 0) {
                        bestAction = actions.get(0);
                    }
                    else {

                        int avgSize = (actions.size() - 1) / (nrThreads - 1);
                        int remain = (actions.size() - 1) - avgSize * (nrThreads - 1);
                        if (finalI - 1 < remain) {
                            int start = (finalI - 1) * (avgSize + 1);
                            int end = start + avgSize + 1;
                            actions = actions.subList(start + 1, end + 1);
                        }
                        else {
                            int start = remain * (avgSize + 1) + (finalI - remain - 1) * avgSize;
                            int end = start + avgSize;
                            actions = actions.subList(start + 1, end + 1);
                        }

                        for (Integer action: actions) {
                            // Calculate index of current action
                            int nrTry = tries[action];
                            if (nrTry == 0) {
                                bestAction = action;
                                break;
                            }
                            double meanReward = rewards[action].doubleValue() / nrTry;
                            double exploration = Math.sqrt(Math.log(nrVisits) / nrTry);
                            // Assess the quality of the action according to policy
                            double quality = meanReward + JoinConfig.EXPLORATION_WEIGHT * exploration;
                            if (quality > bestQuality) {
                                bestAction = action;
                                bestQuality = quality;
                            }
                        }
                    }
                    double armMean = armSampling[bestAction];
                    readWriteLocks[finalI].writeLock().lock();
                    dataOffsets[finalI][bestAction] += armMean;
                    accumulatedReward[finalI][bestAction] = accumulatedReward[finalI][bestAction].add(new BigDecimal(armMean));
                    nrTries[finalI][bestAction]++;
                    readWriteLocks[finalI].writeLock().unlock();

                    if (dataOffsets[finalI][bestAction] >= 1) {
                        isFinished.set(true);
                        System.out.println("Finish id: " + finalI + " " + count);
                        return count;
                    }
                }
                return -1;
            }));
        }

        int count = -1;
        for (int i = 0; i < nrThreads; i++) {
            Future<Integer> futureResult = futures.get(i);
            try {
                count = Math.max(futureResult.get(), count);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();
        return count;
    }

    public static int runDataParallel(int nrThreads, int nrJoined, double[] armSampling) {
        System.out.println("Data Parallel");
        // data parallel
        double[][] dataOffsets = new double[nrThreads][nrJoined];
        double[][] accumulatedReward = new double[nrThreads][nrJoined];
        int[][] nrTries = new int[nrThreads][nrJoined];

        ExecutorService executorService = Executors.newFixedThreadPool(nrThreads);
        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < nrThreads; i++) {
            int finalI = i;
            futures.add(executorService.submit(() -> {
                int count = 0;
                while (true) {
                    count++;
                    int nrVisits = 0;
                    int[] tries = new int[nrJoined];
                    double[] rewards = new double[nrJoined];

                    // collect all statistics
                    for (int tid = 0; tid < nrThreads; tid++) {
                        for (int recAction = 0; recAction < nrJoined; recAction++) {
                            int threadTries = nrTries[tid][recAction];
                            tries[recAction] += threadTries;
                            nrVisits += threadTries;
                            rewards[recAction] += accumulatedReward[tid][recAction];
                        }
                    }

                    int bestAction = -1;
                    double bestQuality = -1;
                    for (int action = 0; action < nrJoined; action++) {
                        // Calculate index of current action
                        int nrTry = tries[action];
                        if (nrTry == 0) {
                            bestAction = action;
                            break;
                        }
                        double meanReward = rewards[action] / nrTry;
                        double exploration = Math.sqrt(Math.log(nrVisits) / nrTry);
                        // Assess the quality of the action according to policy
                        double quality = meanReward + JoinConfig.EXPLORATION_WEIGHT * exploration;
                        if (quality > bestQuality) {
                            bestAction = action;
                            bestQuality = quality;
                        }
                    }
                    double armMean = armSampling[bestAction];
                    dataOffsets[finalI][bestAction] += armMean * nrThreads;
                    accumulatedReward[finalI][bestAction] += armMean;
                    nrTries[finalI][bestAction]++;

                    if (dataOffsets[finalI][bestAction] >= 1) {
                        System.out.println("Finish id: " + finalI + " " + count);
                        break;
                    }
                }

                return count;
            }));
        }
        int count = -1;
        for (int i = 0; i < nrThreads; i++) {
            Future<Integer> futureResult = futures.get(i);
            try {
                count = Math.max(count, futureResult.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();
        return count;
    }

    /**
     * Writes header row of benchmark result file.
     *
     * @param benchOut	channel to benchmark file
     */
    public static void writeBenchHeader(PrintWriter benchOut) {
        benchOut.println("Method\tnrThreads\tnrJoined\tSTD\tCost");
    }

    /**
     * Writes out statistics concerning last query execution
     * into given benchmark result file.
     *
     * @throws Exception
     */
    public static void writeStats(String methods, int nrThreads, int nrJoined, double std, int cost,
                                  PrintWriter benchOut) throws Exception {
        benchOut.println(methods + "\t" + nrThreads + "\t" + nrJoined + "\t" + std + "\t" + cost);
        benchOut.flush();
    }
}
