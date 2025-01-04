package org.example;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class MatrixMultiplicationTask implements Callable<int[][]>, Serializable {
    private final int[][] chunkA;
    private final int[][] matrixB;
    private final String nodeName;

    public MatrixMultiplicationTask(int[][] chunkA, int[][] matrixB, String nodeName) {
        this.chunkA = chunkA;
        this.matrixB = matrixB;
        this.nodeName = nodeName;
    }

    @Override
    public int[][] call() {
        System.out.println("Node executing task: " + nodeName);
        int rows = chunkA.length;
        int cols = matrixB[0].length;
        int size = matrixB.length;
        int[][] result = new int[rows][cols];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                for (int k = 0; k < size; k++) {
                    result[i][j] += chunkA[i][k] * matrixB[k][j];
                }
            }
        }
        return result;
    }
}
