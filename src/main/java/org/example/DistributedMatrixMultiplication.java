package org.example;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;

public class DistributedMatrixMultiplication {

    private HazelcastInstance hazelcastInstance;
    private boolean isMaster;

    public void run() throws ExecutionException, InterruptedException {
        configureHazelcast();
        isMaster = determineMasterNode();

        IMap<Integer, int[][]> distributedMatrixA = hazelcastInstance.getMap("matrixA");
        IMap<Integer, int[][]> distributedMatrixB = hazelcastInstance.getMap("matrixB");

        int[] sizes = {100, 500, 1000, 2000};

        for (int size : sizes) {
            System.out.println("\nTesting with matrices of size: " + size + "x" + size);
            processMatrices(size, distributedMatrixA, distributedMatrixB);
        }
    }

    private void configureHazelcast() {
        Config config = new Config();
        config.setInstanceName("hazelcastInstance");
        config.setClassLoader(Thread.currentThread().getContextClassLoader());

        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .addMember("IP1") // Node 1
                .addMember("IP2"); // Node 2

        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    private boolean determineMasterNode() {
        return hazelcastInstance.getCluster().getMembers().iterator().next().localMember();
    }

    private int[][] generateMatrix(int rows, int cols) {
        int[][] matrix = new int[rows][cols];
        Random random = new Random();
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(10);
            }
        }
        return matrix;
    }

    private void processMatrices(int size, IMap<Integer, int[][]> distributedMatrixA, IMap<Integer, int[][]> distributedMatrixB) throws ExecutionException, InterruptedException {
        if (isMaster) {
            generateAndDistributeMatrices(size, distributedMatrixA, distributedMatrixB);
        } else {
            waitForMatrices(size, distributedMatrixA, distributedMatrixB);
        }

        int[][] matrixA = distributedMatrixA.get(size);
        int[][] matrixB = distributedMatrixB.get(size);

        List<int[][]> chunks = divideMatrix(matrixA, 250);

        int[][] result = executeMatrixMultiplication(chunks, matrixB);

        displayResults(size, result);
    }

    private void generateAndDistributeMatrices(int size, IMap<Integer, int[][]> distributedMatrixA, IMap<Integer, int[][]> distributedMatrixB) {
        System.out.println("I am the master node. Generating matrices...");
        int[][] matrixA = generateMatrix(size, size);
        int[][] matrixB = generateMatrix(size, size);

        distributedMatrixA.put(size, matrixA);
        distributedMatrixB.put(size, matrixB);

        System.out.println("Matrices generated and distributed.");
    }

    private void waitForMatrices(int size, IMap<Integer, int[][]> distributedMatrixA, IMap<Integer, int[][]> distributedMatrixB) throws InterruptedException {
        System.out.println("I am a secondary node. Waiting for matrices...");
        while (!distributedMatrixA.containsKey(size) || !distributedMatrixB.containsKey(size)) {
            Thread.sleep(100);
        }
    }

    private List<int[][]> divideMatrix(int[][] matrix, int chunkSize) {
        List<int[][]> chunks = new ArrayList<>();
        for (int i = 0; i < matrix.length; i += chunkSize) {
            int[][] chunk = new int[Math.min(chunkSize, matrix.length - i)][matrix[0].length];
            System.arraycopy(matrix, i, chunk, 0, chunk.length);
            chunks.add(chunk);
        }
        return chunks;
    }

    private int[][] executeMatrixMultiplication(List<int[][]> chunks, int[][] matrixB) throws ExecutionException, InterruptedException {
        ExecutorService executorService = hazelcastInstance.getExecutorService("matrixExecutor");
        List<Future<int[][]>> futures = new ArrayList<>();

        for (int[][] chunk : chunks) {
            System.out.println("Sending task to the cluster for a chunk of " + chunk.length + " rows.");
            Future<int[][]> future = executorService.submit(new MatrixMultiplicationTask(chunk, matrixB, hazelcastInstance.getCluster().getLocalMember().toString()));
            futures.add(future);
        }

        int[][] result = new int[matrixB.length][matrixB[0].length];
        int currentRow = 0;

        for (Future<int[][]> future : futures) {
            int[][] partialResult = future.get();
            System.arraycopy(partialResult, 0, result, currentRow, partialResult.length);
            currentRow += partialResult.length;
        }
        return result;
    }

    private void displayResults(int size, int[][] result) {
        System.out.println("Multiplication completed for size " + size + ". Partial results:");
        for (int i = 0; i < Math.min(5, result.length); i++) { // Imprimir solo las primeras 5 filas
            for (int j = 0; j < Math.min(5, result[i].length); j++) {
                System.out.print(result[i][j] + " ");
            }
            System.out.println();
        }
    }
}
