/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util.packed;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link DirectMonotonicWriter}.
 *
 * @see DirectMonotonicWriter
 */
public final class DirectMonotonicReader extends LongValues {

  /**
   * In-memory metadata that needs to be kept around for {@link DirectMonotonicReader} to read data
   * from disk.
   */
  public static class Meta {

    // Use a shift of 63 so that there would be a single block regardless of the number of values.
    private static final Meta SINGLE_ZERO_BLOCK = new Meta(1L, 63);

    private final int blockShift;
    private final int numBlocks;
    private final long[] mins;
    private final float[] avgs;
    private final byte[] bpvs;
    private final long[] offsets;

    Meta(long numValues, int blockShift) {
      this.blockShift = blockShift;
      long numBlocks = numValues >>> blockShift;
      if ((numBlocks << blockShift) < numValues) {
        numBlocks += 1;
      }
      this.numBlocks = (int) numBlocks;
      this.mins = new long[this.numBlocks];
      this.avgs = new float[this.numBlocks];
      this.bpvs = new byte[this.numBlocks];
      this.offsets = new long[this.numBlocks];
    }
  }

  /**
   * Load metadata from the given {@link IndexInput}.
   *
   * @see DirectMonotonicReader#getInstance(Meta, RandomAccessInput)
   */
  public static Meta loadMeta(IndexInput metaIn, long numValues, int blockShift)
      throws IOException {
    boolean allValuesZero = true;
    Meta meta = new Meta(numValues, blockShift);
    for (int i = 0; i < meta.numBlocks; ++i) {
      long min = metaIn.readLong();
      meta.mins[i] = min;
      int avgInt = metaIn.readInt();
      meta.avgs[i] = Float.intBitsToFloat(avgInt);
      meta.offsets[i] = metaIn.readLong();
      byte bpvs = metaIn.readByte();
      meta.bpvs[i] = bpvs;
      allValuesZero = allValuesZero && min == 0L && avgInt == 0 && bpvs == 0;
    }
    // save heap in case all values are zero
    return allValuesZero ? Meta.SINGLE_ZERO_BLOCK : meta;
  }

  /** Retrieves a non-merging instance from the specified slice. */
  public static DirectMonotonicReader getInstance(Meta meta, RandomAccessInput data)
      throws IOException {
    return getInstance(meta, data, false);
  }

  /** Retrieves an instance from the specified slice. */
  public static DirectMonotonicReader getInstance(
      Meta meta, RandomAccessInput data, boolean merging) throws IOException {
    return new DirectMonotonicReader(
        data, meta.blockShift, meta.offsets, meta.mins, meta.avgs, meta.bpvs);
  }

  private final int blockShift;
  private final long blockMask;
  private final long[] offsets;
  private final long[] mins;
  private final float[] avgs;
  private final byte[] bpvs;
  private final RandomAccessInput data;

  private DirectMonotonicReader(
      RandomAccessInput data,
      int blockShift,
      long[] offsets,
      long[] mins,
      float[] avgs,
      byte[] bpvs) {
    this.data = data;
    this.blockShift = blockShift;
    this.blockMask = (1L << blockShift) - 1;
    this.offsets = offsets;
    this.mins = mins;
    this.avgs = avgs;
    this.bpvs = bpvs;
    if (offsets.length != mins.length
        || offsets.length != avgs.length
        || offsets.length != bpvs.length) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public long get(long index) {
    final int block = (int) (index >>> blockShift);
    final long blockIndex = index & blockMask;
    final long offset = offsets[block];
    final long delta;
    final int bits = bpvs[block];
    if (bits == 0) {
      delta = 0;
    } else {
      final long readOffset;
      int shift = 0;
      long readMask = ((1L << bits) - 1);
      if (bits == 1) {
        readOffset = blockIndex >>> 3;
        shift = (int) (blockIndex & 7);
      } else if (bits == 2) {
        readOffset = blockIndex >>> 2;
        shift = ((int) (blockIndex & 3)) << 1;
      } else if (bits == 4) {
        shift = (int) (blockIndex & 1) << 2;
        readOffset = (blockIndex >>> 1);
      } else if (bits == 8) {
        readOffset = blockIndex;
      } else if (bits == 12) {
        readOffset = (blockIndex * 12) >>> 3;
        shift = (int) (blockIndex & 1) << 2;
      } else if (bits == 16) {
        readOffset = (blockIndex << 1);
      } else if (bits == 20) {
        readOffset = (blockIndex * 20) >>> 3;
        shift = (int) (blockIndex & 1) << 2;
      } else if (bits == 24) {
        readOffset = blockIndex * 3;
      } else if (bits == 28) {
        readOffset = (blockIndex * 28) >>> 3;
        shift = (int) (blockIndex & 1) << 2;
      } else if (bits == 32) {
        readOffset = blockIndex << 2;
      } else if (bits == 40) {
        readOffset = blockIndex * 5;
      } else if (bits == 48) {
        readOffset = blockIndex * 6;
      } else if (bits == 56) {
        readOffset = blockIndex * 7;
      } else {
        readOffset = blockIndex << 3;
        readMask = ~0;
      }
      final long read;
      final long readFrom = readOffset + offset;
      try {
        if (bits <= 8) {
          read = data.readByte(readFrom);
        } else if (bits <= 16) {
          read = data.readShort(readFrom);
        } else if (bits <= 32) {
          read = data.readInt(readFrom);
        } else {
          read = data.readLong(readFrom);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      delta = (read >>> shift) & readMask;
    }

    return mins[block] + (long) (avgs[block] * blockIndex) + delta;
  }

  /** Get lower/upper bounds for the value at a given index without hitting the direct reader. */
  private long[] getBounds(long index) {
    final int block = Math.toIntExact(index >>> blockShift);
    final long blockIndex = index & blockMask;
    final long lowerBound = mins[block] + (long) (avgs[block] * blockIndex);
    final long upperBound = lowerBound + (1L << bpvs[block]) - 1;
    if (bpvs[block] == 64 || upperBound < lowerBound) { // overflow
      return new long[] {Long.MIN_VALUE, Long.MAX_VALUE};
    } else {
      return new long[] {lowerBound, upperBound};
    }
  }

  /**
   * Return the index of a key if it exists, or its insertion point otherwise like {@link
   * Arrays#binarySearch(long[], int, int, long)}.
   *
   * @see Arrays#binarySearch(long[], int, int, long)
   */
  public long binarySearch(long fromIndex, long toIndex, long key) {
    if (fromIndex < 0 || fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex=" + fromIndex + ",toIndex=" + toIndex);
    }
    long lo = fromIndex;
    long hi = toIndex - 1;

    while (lo <= hi) {
      final long mid = (lo + hi) >>> 1;
      // Try to run as many iterations of the binary search as possible without
      // hitting the direct readers, since they might hit a page fault.
      final long[] bounds = getBounds(mid);
      if (bounds[1] < key) {
        lo = mid + 1;
      } else if (bounds[0] > key) {
        hi = mid - 1;
      } else {
        final long midVal = get(mid);
        if (midVal < key) {
          lo = mid + 1;
        } else if (midVal > key) {
          hi = mid - 1;
        } else {
          return mid;
        }
      }
    }

    return -1 - lo;
  }
}
