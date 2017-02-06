package org.evaluation.utils;

/**
 * copied from org.apache.flink.runtime.operators.testutils.types
 */
import java.util.Random;

import org.apache.flink.util.MutableObjectIterator;

public class RandomIntPairGenerator implements MutableObjectIterator<IntPair>
{
	private final long seed;

	private final long numRecords;

	private Random rnd;

	private long count;


	public RandomIntPairGenerator(long seed) {
		this(seed, Long.MAX_VALUE);
	}

	public RandomIntPairGenerator(long seed, long numRecords) {
		this.seed = seed;
		this.numRecords = numRecords;
		this.rnd = new Random(seed);
	}


	@Override
	public IntPair next(IntPair reuse) {
		if (this.count++ < this.numRecords) {
			reuse.setKey(this.rnd.nextInt());
			reuse.setValue(this.rnd.nextInt());
			return reuse;
		} else {
			return null;
		}
	}

	@Override
	public IntPair next() {
		if (this.count++ < this.numRecords) {
			return new IntPair(this.rnd.nextInt(), this.rnd.nextInt());
		} else {
			return null;
		}
	}

	public void reset() {
		this.rnd = new Random(this.seed);
		this.count = 0;
	}
}

