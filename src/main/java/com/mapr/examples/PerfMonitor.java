package com.mapr.examples;

/**
 * Created by idownard on 7/27/16.
 */
public class PerfMonitor {
    public static void print_status(long records_processed, long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
        System.out.printf("Throughput = %.2f msgs/sec. Message count = %d\n", records_processed / ((double) elapsedTime / 1000000000.0), records_processed);
    }
}
