package org.example;

import org.apache.commons.collections.BufferOverflowException;
import org.apache.flink.api.common.io.SerializedOutputFormat;
import org.apache.flink.api.java.io.SplitDataProperties;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.MutableObjectIterator;
import org.example.utils.Log;
import org.example.utils.SortHandler;
import org.example.utils.SorterFactory;
import org.example.utils.Validator;
import org.example.utils.generator.RandomTuple2LongInt;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class SimpleApp {

	private static boolean wait_for_start = Boolean.parseBoolean(System.getenv("WAIT_FOR_START"));

	public static void main(String[] args) throws Exception {

		// 1st args is sorter
		String sorterClass = "org.apache.flink.runtime.operators.sort.NormalizedKeySorter";
		if( args.length >= 1 ) {
			sorterClass = args[0];
		}

		printMachineInfo();

		Log.debug("Instantiate sorter : " + sorterClass );


		InMemorySorter sorter = SorterFactory.getSorter(sorterClass);

		fillRandomData(sorter);

		waitForUser("Press Enter to sort");

		QuickSort qs = new QuickSort();

		long start_time = System.nanoTime();

		qs.sort(sorter);

		long end_time = System.nanoTime();
		double difference = (end_time - start_time)/1e6;
		System.out.println("sorting time : " + difference +"ms");
		waitForUser("Press Enter to continue");

		boolean isSorted = Validator.isSorted(sorter.getIterator());
		if( isSorted ) {
			Log.debug("Data is sorted correctly");
		} else {
			throw new Error("Data is NOT sorted properly");
		}



		//TODO: output result to destination file.
	}

	public static void fillRandomData(InMemorySorter sorter) throws IOException {
		RandomTuple2LongInt generator = new RandomTuple2LongInt(Configuration.SEED);

		Tuple2<Long,Integer> record = new Tuple2<Long,Integer>();
		int num = 0;

		Log.debug("Randomly generating " + Configuration.NUM_RECORDS + " records");

		while (num < Configuration.NUM_RECORDS){
			generator.next(record);
			boolean succeed = sorter.write(record);
			if( !succeed ) {
				throw new BufferOverflowException(" We have enough space for " + num  + "records only.");
			}
			num++;
		}
	}

	static void waitForUser(String msg) throws IOException {
		if( wait_for_start ) {
			InputStreamReader r = new InputStreamReader(System.in);
			Log.debug(msg);
			new BufferedReader(r).readLine();
		}
	}

	static void printMachineInfo(){
		Log.debug("Endianness : " + ByteOrder.nativeOrder() );
		Log.debug("-----------------------------");
	}

}
