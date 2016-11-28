package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.example.utils.SorterFactory;
import org.example.utils.generator.RandomIntTuple2;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


//import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.*;
//import org.openjdk.jmh.annotations.Scope;
//import org.openjdk.jmh.annotations.State;
//import org.openjdk.jmh.annotations.Warmup;

@OutputTimeUnit(TimeUnit.SECONDS)

/**
 * Created by heytitle on 11/28/16.
 */
public class Benchmarker {

	@Benchmark
	@Warmup(iterations = Configuration.BENCHMARK_WARMUP)
	@Measurement(iterations = Configuration.BENCHMARK_ITERATION)
	public void normalizedKeySort() throws IOException {
		InMemorySorter sorter = SorterFactory.createSorter();
		this.writeRandomData(sorter);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);
	}

	public static void writeRandomData(InMemorySorter sorter) throws IOException {
		RandomIntTuple2 generator = new RandomIntTuple2(Configuration.SEED);
		int num = -1;
		Tuple2<Integer,Integer> record = new Tuple2<Integer,Integer>();
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < Configuration.BENCHMARK_RECORD_SIZE);
	}
}
