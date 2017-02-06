package org.evaluation;

/**
 * Created by heytitle on 11/28/16.
 */
public class Configuration {
	public static final long NUM_RECORDS = 10000000;
	public static final int pageSize     = 256*2048; // bytes
	public static final int numSegments  = 1000;
	public static final int SEED	     = 11;

	public static final int BENCHMARK_WARMUP = 20;
	public static final int BENCHMARK_ITERATION = 100;
}
