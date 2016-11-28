package org.example.utils.generator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.Random;

/**
 * Created by heytitle on 11/28/16.
 */
public class RandomIntTuple2 implements MutableObjectIterator<Tuple2<Integer,Integer>> {

	private final long seed;

	private final long numRecords;

	private Random rnd;

	private long count;


	public RandomIntTuple2(long seed) {
		this(seed, Long.MAX_VALUE);
	}

	public RandomIntTuple2(long seed, long numRecords) {
		this.seed = seed;
		this.numRecords = numRecords;
		this.rnd = new Random(seed);
	}

	@Override
	public Tuple2<Integer,Integer> next(Tuple2<Integer,Integer> reuse) {
		if (this.count++ < this.numRecords) {
			reuse.setField(this.rnd.nextInt(), 0);
			reuse.setField(this.rnd.nextInt(), 1);
			return reuse;
		} else {
			return null;
		}
	}

	@Override
	public Tuple2<Integer,Integer> next() {
		if (this.count++ < this.numRecords) {
			return new Tuple2(this.rnd.nextInt(), this.rnd.nextInt());
		} else {
			return null;
		}
	}

	public void reset() {
		this.rnd = new Random(this.seed);
		this.count = 0;
	}
}
