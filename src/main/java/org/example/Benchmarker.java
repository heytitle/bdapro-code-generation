package org.example;

import com.esotericsoftware.kryo.Kryo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.MutableObjectIterator;
import org.example.utils.SorterFactory;
import org.example.utils.generator.RandomTuple2LongInt;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;


//import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.*;
//import org.openjdk.jmh.annotations.Scope;
//import org.openjdk.jmh.annotations.State;
//import org.openjdk.jmh.annotations.Warmup;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
/**
 * Created by heytitle on 11/28/16.
 */
public class Benchmarker {


	@Benchmark
	@BenchmarkMode({Mode.SingleShotTime})
	@Warmup(iterations = Configuration.BENCHMARK_WARMUP)
	@Measurement(iterations = Configuration.BENCHMARK_ITERATION)
	public MutableObjectIterator testSort(ThreadState ts ) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		ts.quickSort.sort(ts.sorter);
		return ts.sorter.getIterator();
	}


	@State(Scope.Thread)
	public static class ThreadState {
		@Param({"org.apache.flink.runtime.operators.sort.NormalizedKeySorter", "org.example.MySorter"})
		public String sorterClass;

		@Param({"10000", "20000", "50000"})
		public int noRecords;

		volatile InMemorySorter sorter;
		volatile QuickSort quickSort;

		@Setup(Level.Iteration)
		public void writeRandomData() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
			sorter = SorterFactory.getSorter(sorterClass);
			quickSort = new QuickSort();

			RandomTuple2LongInt generator = new RandomTuple2LongInt(Configuration.SEED);

			int num = -1;
			Tuple2<Long,Integer> record = new Tuple2<Long,Integer>();
			do {
				generator.next(record);
				num++;
			}
			while (sorter.write(record) && num < noRecords );
		}
	}
}
