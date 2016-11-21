package org.example;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;

import java.util.ArrayList;

public class SimpleApp {
	private static TypeSerializer<Tuple2<Integer,Integer>> serializer;

	public static void main(String[] args) {

		System.out.println("HI!");

		ArrayList<Tuple2<Integer,Integer>> dummyData = generateDummyData(10);
		System.out.println(dummyData);

		TypeInformation<?>[] fieldTypes = new TypeInformation[2];
		fieldTypes[0] = TypeInformation.of(Integer.class);
		fieldTypes[1] = TypeInformation.of(Integer.class);

		serializer = new TupleSerializer<Tuple2<Integer, Integer>>(
				(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
				new TypeSerializer[] {
						IntSerializer.INSTANCE,
						IntSerializer.INSTANCE
				}
		);



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

	private InMemorySorter createSorter(){

//		FixedLengthRecordSorter<IntPair> b = new FixedLengthRecordSorter<IntPair>()

	}
}
