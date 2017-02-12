package org.evaluation.utils;

/**
 * Created by heytitle on 11/28/16.
 */

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.evaluation.Configuration;
import org.apache.flink.core.memory.MyMemorySegment;
import org.org.apache.flink.api.common.typeutils.base.LongComparatorLittleEndian;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class SorterFactory {


	public static InMemorySorter getSorter( String sorterName ) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

		TypeSerializer[] insideSerializers = new TypeSerializer[] {
			IntSerializer.INSTANCE,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Integer,Integer>> serializer = new TupleSerializer<Tuple2<Integer, Integer>>(
			(Class<Tuple2<Integer, Integer>>) (Class<?>) Tuple2.class,
			insideSerializers
		);

		TypeComparator[] typeComp = null;
		if( sorterName.equals("org.evaluation.sorter.individual.optimization.UseLittleEndian")
				|| ( sorterName.equals("org.evaluation.sorter.OptimizedSorter") && ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ) ) {
			typeComp = new TypeComparator[] {
				new IntComparator(true)
			};
		} else {
			typeComp = new TypeComparator[] {
				new IntComparator(true)
			};
		}

		TupleComparator<Tuple2<Integer,Integer>> comparator = new TupleComparator<Tuple2<Integer, Integer>>(
			new int[]{0}, typeComp, insideSerializers
		);

		Class sorterClass = Class.forName(sorterName);

		Class[] types = {
			TypeSerializer.class, TypeComparator.class, List.class
		};

		Constructor constructor = sorterClass.getConstructor(types);

		Object[] parameters = {
			serializer, comparator, getMemory( sorterName )
		};

		InMemorySorter<Tuple2<Long,Integer>> sorter = (InMemorySorter<Tuple2<Long,Integer>>) constructor.newInstance(parameters);

		return  sorter;
	}

	public static List<MemorySegment> getMemory(String sorterName) {
		ArrayList<MemorySegment> list = new ArrayList<MemorySegment>(Configuration.numSegments);
		for (int i = 0; i < Configuration.numSegments; i++) {
			MemorySegment mem = null;
			if( sorterName.equals("org.example.MySorter")) {
				mem = MyMemorySegment.FACTORY.allocateUnpooledSegment( Configuration.pageSize, (Object) null );
			} else {
				mem = MemorySegmentFactory.allocateUnpooledSegment( Configuration.pageSize, (Object) null);
			}
			list.add(mem);
		}
		return list;
	}

}