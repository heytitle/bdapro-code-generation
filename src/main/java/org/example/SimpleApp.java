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
	private static final int noBlocks    = 32; // 2^(a-10)*(Indexsize) in this case indexsize = 4+8
	private static final int pageSize    = noBlocks*1024; // this makes indexEntriesPerSegment be exponential of 2 ( 2^11 )
	private static final int numSegments = 34;
	private static final int NUM_RECORDS = 10;
	private static final int SEED	     = 11;

	// TODO:
	// create sorter
	// @preparephase
	// 	 preparedata
	// do sort

	public static void main(String[] args) throws Exception {

		RandomTuple2LongInt generator = new RandomTuple2LongInt(SEED);

//		InMemorySorter sorter = SorterFactory.getSorter("org.apache.flink.runtime.operators.sort.NormalizedKeySorter");
		InMemorySorter sorter = SorterFactory.getSorter("org.example.MySorter");

		int num = -1;
		Tuple2<Long,Integer> record = new Tuple2<Long,Integer>();
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		checkCorrectness(sorter.getIterator());
	}


	private static List<MemorySegment> getMemory(int numSegments, int segmentSize) {
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(numSegments);
		for (int i = 0; i < numSegments; i++) {
			list.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
		}
		return list;
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
