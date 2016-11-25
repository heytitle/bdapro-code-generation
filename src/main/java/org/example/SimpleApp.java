package org.example;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.MutableObjectIterator;
import org.example.utils.IntPair;
import org.example.utils.IntPairComparator;
import org.example.utils.IntPairSerializer;
import org.example.utils.RandomIntPairGenerator;
import scala.Int;


import java.util.ArrayList;
import java.util.List;

public class SimpleApp {
	private static final int pageSize = 32*1024;
	private static final int numSegments = 34;
	private static final int NUM_RECORDS = 10;
	private static final int SEED	     = 11;


	public static void main(String[] args) throws Exception {

		RandomIntPairGenerator generator = new RandomIntPairGenerator(SEED);

		InMemorySorter sorter = createSorter();

		int num = -1;
		IntPair record = new IntPair();
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		checkCorrectness(sorter.getIterator());
	}

	public static InMemorySorter createSorter() {


		IntPairSerializer serializer = new IntPairSerializer();
		IntPairComparator comparator = new IntPairComparator();
		MemorySegment memory = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

		FixedLengthRecordSorter<IntPair> sorter = new FixedLengthRecordSorter<IntPair>(
			serializer,
			comparator,
			getMemory( numSegments, pageSize )
		);

		return  sorter;
	}


	private static List<MemorySegment> getMemory(int numSegments, int segmentSize) {
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(numSegments);
		for (int i = 0; i < numSegments; i++) {
			list.add(MemorySegmentFactory.allocateUnpooledSegment(segmentSize));
		}
		return list;
	}

	private static void checkCorrectness(MutableObjectIterator<IntPair> iter) throws Exception{
		IntPair readTarget = new IntPair();

		int current;
		int last;

		iter.next(readTarget);
		last = readTarget.getKey();

		while ((readTarget = iter.next(readTarget)) != null) {
			current = readTarget.getKey();

			final int cmp = last - current;
			if (cmp > 0) {
				throw new Exception("Data was not sorted properly");
			}
			last = current;
		}

		System.out.println("checkCorrectness: passed");

	}

}
