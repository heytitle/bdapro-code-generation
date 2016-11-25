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

import java.util.ArrayList;
import java.util.List;

public class SimpleApp {
	private static final int pageSize = 32*1024;
	private static final int numSegments = 34;


	public static void main(String[] args) {

		System.out.println("HI!");

		ArrayList<Tuple2<Integer,Integer>> dummyData = generateDummyData(10);
		System.out.println(dummyData);


		System.out.print("Creating sorter");

		InMemorySorter sorter = createSorter();




	}

	private	static ArrayList<Tuple2<Integer,Integer>> generateDummyData(int size ){
		ArrayList<Tuple2<Integer,Integer>> data = new ArrayList<Tuple2<Integer,Integer>>();

		while(size > 0 ){
			Tuple2 t = new Tuple2(1,2);
			data.add(t);
			size--;
		}

		return data;
	}

	public static InMemorySorter createSorter() {
		TypeSerializer[] typeSerializer = new TypeSerializer[] {
				IntSerializer.INSTANCE,
				IntSerializer.INSTANCE
		};

		TypeSerializer<Tuple2<Integer,Integer>> tupleSerializer = new TupleSerializer<Tuple2<Integer, Integer>>(
				(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
				typeSerializer
		);


		IntComparator intComparator = new IntComparator(true);

		TupleComparator<Tuple2<Integer,Integer>> comparator = new TupleComparator<Tuple2<Integer, Integer>>(
				new int[]{0},
				new TypeComparator[]{
					intComparator
				},
				typeSerializer
		);

		MemorySegment memory = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

		FixedLengthRecordSorter<Tuple2<Integer,Integer>> sorter = new FixedLengthRecordSorter<Tuple2<Integer, Integer>>(
			tupleSerializer,
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

}
