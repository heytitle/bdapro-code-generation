package org.example;

import org.apache.commons.collections.BufferOverflowException;
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

		fillRandomData(sorter);

		QuickSort qs = new QuickSort();

		qs.sort(sorter);

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

}
