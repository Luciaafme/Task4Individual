package org.example;

public class Main {
    public static void main(String[] args) {
        try {
            DistributedMatrixMultiplication app = new DistributedMatrixMultiplication();
            app.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
