package org.apache.flink.core.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Created by heytitle on 12/4/16.
 */
public class MyMemorySegment extends MemorySegment {
	public static final MyMemorySegment.MyMemorySegmentFactory FACTORY = new MyMemorySegment.MyMemorySegmentFactory();
	private byte[] memory;

	public MyMemorySegment(byte[] memory) {
		this(memory, null);
	}



	MyMemorySegment(byte[] memory, Object owner) {
		super(Objects.requireNonNull(memory),owner);
		this.memory = memory;
	}

	@Override
	public void free() {
		super.free();
		this.memory = null;
	}

	@Override
	public ByteBuffer wrap(int offset, int length) {
		try {
			return ByteBuffer.wrap(this.memory, offset, length);
		}
		catch (NullPointerException e) {
			throw new IllegalStateException("segment has been freed");
		}
	}

	/**
	 * Gets the byte array that backs this memory segment.
	 *
	 * @return The byte array that backs this memory segment, or null, if the segment has been freed.
	 */
	public byte[] getArray() {
		return this.heapMemory;
	}

	// ------------------------------------------------------------------------
	//                    Random Access get() and put() methods
	// ------------------------------------------------------------------------

	@Override
	public final byte get(int index) {
		return this.memory[index];
	}

	@Override
	public final void put(int index, byte b) {
		this.memory[index] = b;
	}

	@Override
	public final void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}

	@Override
	public final void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}

	@Override
	public final void get(int index, byte[] dst, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(this.memory, index, dst, offset, length);
	}

	@Override
	public final void put(int index, byte[] src, int offset, int length) {
		// system arraycopy does the boundary checks anyways, no need to check extra
		System.arraycopy(src, offset, this.memory, index, length);
	}

	@Override
	public final boolean getBoolean(int index) {
		return this.memory[index] != 0;
	}

	@Override
	public final void putBoolean(int index, boolean value) {
		this.memory[index] = (byte) (value ? 1 : 0);
	}

	// -------------------------------------------------------------------------
	//                     Bulk Read and Write Methods
	// -------------------------------------------------------------------------

	@Override
	public final void get(DataOutput out, int offset, int length) throws IOException {
		out.write(this.memory, offset, length);
	}

	@Override
	public final void put(DataInput in, int offset, int length) throws IOException {
		in.readFully(this.memory, offset, length);
	}

	@Override
	public final void get(int offset, ByteBuffer target, int numBytes) {
		// ByteBuffer performs the boundary checks
		target.put(this.memory, offset, numBytes);
	}

	@Override
	public final void put(int offset, ByteBuffer source, int numBytes) {
		// ByteBuffer performs the boundary checks
		source.get(this.memory, offset, numBytes);
	}

	public final void fastSwapBytes(byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
//		if((offset1 | offset2 | len | tempBuffer.length - len) >= 0) {
			long thisPos = this.address + (long)offset1;
			long otherPos = seg2.address + (long)offset2;
//			if(thisPos <= this.addressLimit - (long)len && otherPos <= seg2.addressLimit - (long)len) {
				UNSAFE.copyMemory(this.heapMemory, thisPos, tempBuffer, BYTE_ARRAY_BASE_OFFSET, (long)len);
				UNSAFE.copyMemory(seg2.heapMemory, otherPos, this.heapMemory, thisPos, (long)len);
				UNSAFE.copyMemory(tempBuffer, BYTE_ARRAY_BASE_OFFSET, seg2.heapMemory, otherPos, (long)len);
				return;
//			}

//			if(this.address > this.addressLimit) {
//				throw new IllegalStateException("this memory segment has been freed.");
//			}
//
//			if(seg2.address > seg2.addressLimit) {
//				throw new IllegalStateException("other memory segment has been freed.");
//			}
//		}
//
//		throw new IndexOutOfBoundsException(String.format("offset1=%d, offset2=%d, len=%d, bufferSize=%d, address1=%d, address2=%d", new Object[]{Integer.valueOf(offset1), Integer.valueOf(offset2), Integer.valueOf(len), Integer.valueOf(tempBuffer.length), Long.valueOf(this.address), Long.valueOf(seg2.address)}));
	}


	public static final class MyMemorySegmentFactory implements MemorySegmentFactory.Factory {
		public MyMemorySegment wrap(byte[] memory) {
			return new MyMemorySegment(memory);
		}

		public MyMemorySegment allocateUnpooledSegment(int size, Object owner) {
			return new MyMemorySegment(new byte[size], owner);
		}

		public MyMemorySegment wrapPooledHeapMemory(byte[] memory, Object owner) {
			return new MyMemorySegment(memory, owner);
		}

		public MyMemorySegment wrapPooledOffHeapMemory(ByteBuffer memory, Object owner) {
			throw new UnsupportedOperationException("The MemorySegment factory was not initialized for off-heap memory.");
		}
	}
}
