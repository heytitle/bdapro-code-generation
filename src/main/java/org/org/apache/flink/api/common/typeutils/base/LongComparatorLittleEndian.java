package org.org.apache.flink.api.common.typeutils.base;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.base.BasicTypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

@Internal
public final class LongComparatorLittleEndian extends BasicTypeComparator<Long> {

	private static final long serialVersionUID = 1L;


	public LongComparatorLittleEndian(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		long l1 = firstSource.readLong();
		long l2 = secondSource.readLong();
		int comp = (l1 < l2 ? -1 : (l1 == l2 ? 0 : 1));
		return ascendingComparison ? comp : -comp;
	}


	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 8;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 8;
	}

	@Override
	public void putNormalizedKey(Long lValue, MemorySegment target, int offset, int numBytes) {
		long value = lValue.longValue() - Long.MIN_VALUE;

		// see IntValue for an explanation of the logic
		if (numBytes == 8) {
			// default case, full normalized key
			target.putLong(offset,value);
		}
		else if (numBytes <= 0) {
		}
		else if (numBytes < 8) {
			for (int i = 0; numBytes > 0; numBytes--, i++) {
				target.put(offset + i, (byte) (value >>> ((7-i)<<3)));
			}
		}
		else {
			target.putLong(offset, value);
			for (int i = 8; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}


	@Override
	public LongComparatorLittleEndian duplicate() {
		return new LongComparatorLittleEndian(ascendingComparison);
	}
}
