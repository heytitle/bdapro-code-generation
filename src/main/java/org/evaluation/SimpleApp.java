package org.evaluation;

import org.apache.commons.collections.BufferOverflowException;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.evaluation.utils.Log;
import org.evaluation.utils.SorterFactory;
import org.evaluation.utils.Validator;
import org.evaluation.utils.generator.RandomTuple2LongInt;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteOrder;

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


		InMemorySorter sorter = null;

		sorter  = SorterFactory.getSorter(sorterClass);

		fillRandomData(sorter);

		QuickSort qs = new QuickSort();

		waitForUser("Press Enter to sort");
		long start_time = System.nanoTime();

		qs.sort(sorter);

		long end_time = System.nanoTime();
		double difference = (end_time - start_time)/1e6;
		System.out.println( ": sorting time : " + difference +"ms");


		waitForUser("Press Enter to continue");

		boolean isSorted = Validator.isSorted(sorter.getIterator());
		if( isSorted ) {
			Log.debug("Data is sorted correctly");
		} else {
			throw new Error("Data is NOT sorted properly");
		}
	}

	public static void fillRandomData(InMemorySorter sorter) throws IOException {
		RandomTuple2LongInt generator = new RandomTuple2LongInt(Configuration.SEED);

		Tuple2<Integer,Integer> record = new Tuple2<Integer, Integer>();
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
