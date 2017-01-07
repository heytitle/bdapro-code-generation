package org.example.utils;

/**
 * Created by heytitle on 11/28/16.
 */

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.example.Configuration;
import org.apache.flink.core.memory.MyMemorySegment;
import org.org.apache.flink.api.common.typeutils.base.LongComparatorLittleEndian;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class SorterFactory {


	public static InMemorySorter getSorter( String sorterName ) throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

		TypeSerializer[] insideSerializers = new TypeSerializer[] {
			LongSerializer.INSTANCE,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Long,Integer>> serializer = new TupleSerializer<Tuple2<Long, Integer>>(
			(Class<Tuple2<Long, Integer>>) (Class<?>) Tuple2.class,
			insideSerializers
		);

		TypeComparator[] typeComp = null;
		if( sorterName.equals("org.example.sorter.UseLittleEndian") ) {
			typeComp = new TypeComparator[] {
				new LongComparatorLittleEndian(true)
			};
		} else {
			typeComp = new TypeComparator[] {
				new LongComparator(true)
			};
		}

		TupleComparator<Tuple2<Long,Integer>> comparator = new TupleComparator<Tuple2<Long, Integer>>(
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

	private static List<MemorySegment> getMemory(String sorterName) {
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