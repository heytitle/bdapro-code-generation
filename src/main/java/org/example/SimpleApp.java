package org.example;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.MutableObjectIterator;
import org.example.utils.SorterFactory;
import org.example.utils.generator.RandomTuple2LongInt;


import java.util.ArrayList;
import java.util.List;

public class SimpleApp {

	public static void main(String[] args) throws Exception {
		RandomTuple2LongInt generator = new RandomTuple2LongInt(Configuration.SEED);

//		InMemorySorter sorter = SorterFactory.getSorter("org.apache.flink.runtime.operators.sort.NormalizedKeySorter");
		InMemorySorter sorter = SorterFactory.getSorter("org.example.MySorter");

		int num = -1;
		Tuple2<Long,Integer> record = new Tuple2<Long,Integer>();
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < Configuration.NUM_RECORDS);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		checkCorrectness(sorter.getIterator());
	}

	private static void checkCorrectness(MutableObjectIterator<Tuple2<Long,Integer>> iter) throws Exception{
		Tuple2<Long,Integer> readTarget = new Tuple2<Long,Integer>();

		long current;
		long last;

		iter.next(readTarget);
		last = readTarget.getField(0);

		while ((readTarget = iter.next(readTarget)) != null) {
			current = readTarget.getField(0);

			final long cmp = last - current;
			if (cmp > 0) {
				throw new Exception("Data was not sorted properly");
			}
			last = current;
		}

		System.out.println("checkCorrectness: passed");

	}

}
