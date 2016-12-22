package org.example.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * Created by heytitle on 12/21/16.
 */
public class Validator {

	public static boolean isSorted(MutableObjectIterator<Tuple2<Long,Integer>> iter) throws IOException {

		Tuple2<Long,Integer> readTarget = new Tuple2<Long,Integer>();

		iter.next(readTarget);

		int num = 0;
		Long prev = null;
		Long current;

		do {
			current = readTarget.getField(0);

			if( prev == null ){
				prev = current;
				continue;
			}


			final long cmp = prev - current;

			if (cmp > 0) {
				return false;
			}

			prev = current;

			num++;

		} while( (readTarget = iter.next(readTarget)) != null ) ;

		return true;
	}
}
