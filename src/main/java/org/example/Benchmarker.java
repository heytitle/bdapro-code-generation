package org.example;

import com.esotericsoftware.kryo.Kryo;
import org.apache.commons.collections.BufferOverflowException;
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
		long start_time = System.nanoTime();

		ts.quickSort.sort(ts.sorter);

		long end_time = System.nanoTime();
		double difference = (end_time - start_time)/1e6;
		System.out.println(" | sort: " + difference +"ms");

		return ts.sorter.getIterator();
	}


	@State(Scope.Thread)
	public static class ThreadState {
		@Param({"org.apache.flink.runtime.operators.sort.NormalizedKeySorter", "org.example.MySorter"})
		public String sorterClass;

		@Param({"10000", "100000", "1000000"})
		public long noRecords;

		volatile InMemorySorter sorter;
		volatile QuickSort quickSort;

		@Setup(Level.Invocation)
		public void writeRandomData() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
			sorter = SorterFactory.getSorter(sorterClass);
			quickSort = new QuickSort();

			long start_time = System.nanoTime();

			RandomTuple2LongInt generator = new RandomTuple2LongInt(Configuration.SEED);

			int num = -1;
			Tuple2<Long,Integer> record = new Tuple2<Long,Integer>();

			while (num < noRecords){
				generator.next(record);
				boolean succeed = sorter.write(record);
				if( !succeed ) {
					throw new BufferOverflowException("Buffer space is not sufficient, only " + num  + "records can be added only.");
				}
				num++;
			}

			long end_time = System.nanoTime();
			double difference = (end_time - start_time)/1e6;

			System.out.print(" setup " + noRecords + " records : " + difference +"ms");
		}
	}
}
