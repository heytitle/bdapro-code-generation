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
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.MutableObjectIterator;
import org.example.utils.generator.RandomIntTuple2;


import java.util.ArrayList;
import java.util.List;

public class SimpleApp {
	private static final int pageSize    = 32*1024;
	private static final int numSegments = 34;
	private static final int NUM_RECORDS = 10;
	private static final int SEED	     = 11;


	public static void main(String[] args) throws Exception {

		RandomIntTuple2 generator = new RandomIntTuple2(SEED);

		InMemorySorter sorter = createSorter();

		int num = -1;
		Tuple2<Integer,Integer> record = new Tuple2<Integer,Integer>();
		do {
			generator.next(record);
			System.out.println(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		checkCorrectness(sorter.getIterator());
	}

	public static InMemorySorter createSorter() {

		TupleSerializer<Tuple2<Integer,Integer>> serializer = new TupleSerializer<Tuple2<Integer, Integer>>(
				(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
				new TypeSerializer[] {
						IntSerializer.INSTANCE,
						IntSerializer.INSTANCE
				}
		);

		TupleComparator<Tuple2<Integer,Integer>> comparator = new TupleComparator<Tuple2<Integer, Integer>>(
				new int[]{0},
				new TypeComparator[]{
					new IntComparator(true)
				},
				new TypeSerializer[] {
					IntSerializer.INSTANCE
				}
		);

		MemorySegment memory = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

		NormalizedKeySorter<Tuple2<Integer,Integer>> sorter = new NormalizedKeySorter<Tuple2<Integer,Integer>>(
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

	private static void checkCorrectness(MutableObjectIterator<Tuple2<Integer,Integer>> iter) throws Exception{
		Tuple2<Integer,Integer> readTarget = new Tuple2<Integer,Integer>();

		int current;
		int last;

		iter.next(readTarget);
		last = readTarget.getField(0);

		while ((readTarget = iter.next(readTarget)) != null) {
			current = readTarget.getField(0);

			final int cmp = last - current;
			if (cmp > 0) {
				throw new Exception("Data was not sorted properly");
			}
			last = current;
		}

		System.out.println("checkCorrectness: passed");

	}

}
