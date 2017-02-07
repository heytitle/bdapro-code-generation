package org.evaluation.utils;

/**
 * copied from org.apache.flink.runtime.operators.testutils.types;
 */
import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

@SuppressWarnings("rawtypes")
public class IntPairComparator extends TypeComparator<IntPair> {

	private static final long serialVersionUID = 1L;

	private int reference;

	private final TypeComparator[] comparators = new TypeComparator[] {new IntComparator(true)};

	@Override
	public int hash(IntPair object) {
		return object.getKey() * 73;
	}

	@Override
	public void setReference(IntPair toCompare) {
		this.reference = toCompare.getKey();
	}

	@Override
	public boolean equalToReference(IntPair candidate) {
		return candidate.getKey() == this.reference;
	}

	@Override
	public int compareToReference(TypeComparator<IntPair> referencedAccessors) {
		final IntPairComparator comp = (IntPairComparator) referencedAccessors;
		return comp.reference - this.reference;
	}

	@Override
	public int compare(IntPair first, IntPair second) {
		return first.getKey() - second.getKey();
	}

	@Override
	public int compareSerialized(DataInputView source1, DataInputView source2) throws IOException {
		return source1.readInt() - source2.readInt();
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 4;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 4;
	}

	@Override
	public void putNormalizedKey(IntPair record, MemorySegment target, int offset, int len) {
		// see IntValue for a documentation of the logic
		final int value = record.getKey() - Integer.MIN_VALUE;

		if (len == 4) {
			target.putIntBigEndian(offset, value);
		}
		else if (len <= 0) {
		}
		else if (len < 4) {
			for (int i = 0; len > 0; len--, i++) {
				target.put(offset + i, (byte) ((value >>> ((3-i)<<3)) & 0xff));
			}
		}
		else {
			target.putIntBigEndian(offset, value);
			for (int i = 4; i < len; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public IntPairComparator duplicate() {
		return new IntPairComparator();
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = ((IntPair) record).getKey();
		return 1;
	}

	@Override public TypeComparator[] getFlatComparators() {
		return comparators;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return true;
	}

	@Override
	public void writeWithKeyNormalization(IntPair record, DataOutputView target) throws IOException {
		target.writeInt(record.getKey() - Integer.MIN_VALUE);
		target.writeInt(record.getValue());
	}

	@Override
	public IntPair readWithKeyDenormalization(IntPair reuse, DataInputView source) throws IOException {
		reuse.setKey(source.readInt() + Integer.MIN_VALUE);
		reuse.setValue(source.readInt());
		return reuse;
	}
}
