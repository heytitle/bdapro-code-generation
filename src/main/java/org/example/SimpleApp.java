package org.example;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.MutableObjectIterator;
import org.example.utils.Log;
import org.example.utils.SorterFactory;
import org.example.utils.generator.RandomTuple2LongInt;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleApp {

	public static void main(String[] args) throws Exception {

//		InMemorySorter sorter = SorterFactory.getSorter("org.apache.flink.runtime.operators.sort.NormalizedKeySorter");
		InMemorySorter sorter = SorterFactory.getSorter("org.example.MySorter");


//		((MySorter) sorter).debug();
//		fillTestData(sorter);
		fillRandomData(sorter);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		checkCorrectness(sorter.getIterator());
	}

	private static void checkCorrectness(MutableObjectIterator<Tuple2<Long,Integer>> iter) throws Exception{
		Tuple2<Long,Integer> readTarget = new Tuple2<Long,Integer>();


		iter.next(readTarget);

		int num = 0;
		Long prev = null;
		Long current;

		boolean isCorrect = true;
		do {
			current = readTarget.getField(0);

			if( prev == null ){
				prev = current;
				continue;
			}

//			Log.debug("## (" + num +") : " + current);

			final long cmp = prev - current;
			if (cmp > 0) {
				isCorrect = false;
				throw new Exception("Data was not sorted properly");
			}

			prev = current;

			num++;

		} while( (readTarget = iter.next(readTarget)) != null ) ;

		if( isCorrect ) {
			System.out.println("checkCorrectness: passed");
		} else {
			System.out.println("checkCorrectness: failed");
		}

	}


	public static  void fillTestData(InMemorySorter sorter) throws IOException {
		long[] data = new long[]{
			-8394919090773124087L,
			0L,
			1L,
			-8395231785417752088L,
			5L,
			1000L
		};


//		sorter = (MySorter)

		Tuple2<Long,Integer> tup;
		for(int i = 0; i < data.length;i++) {
			tup = new Tuple2<Long, Integer>();
			tup.setField(data[i],0);
			System.out.println("added key (" + i +")+ "+  data[i]);
			tup.setField(777,1);
			sorter.write(tup);
		}


	}

	public static void fillRandomData(InMemorySorter sorter) throws IOException {
		RandomTuple2LongInt generator = new RandomTuple2LongInt(Configuration.SEED);

		Tuple2<Long,Integer> record = new Tuple2<Long,Integer>();
		int num = 0;

		Log.debug("Randomly generating " + Configuration.NUM_RECORDS + " records");

		do {
			generator.next(record);
//			Log.debug(" " + (num+1) + " " + record.getField(0) );
			num++;
		}
		while (sorter.write(record) && num < Configuration.NUM_RECORDS);
	}

}
