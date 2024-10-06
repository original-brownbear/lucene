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
    }
  }

  private static final DirectMonotonicReader SINGLE_ZERO_BLOCK_READER =
      createInstance(Meta.SINGLE_ZERO_BLOCK, IndexInput.Empty.INSTANCE, false);

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
      metaIn.skipBytes(Long.BYTES);
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
      Meta meta, RandomAccessInput data, boolean merging) {
    if (meta == Meta.SINGLE_ZERO_BLOCK) {
      return SINGLE_ZERO_BLOCK_READER;
    }
    return createInstance(meta, data, merging);
  }

  private static DirectMonotonicReader createInstance(
      Meta meta, RandomAccessInput data, boolean merging) {
    final LongValues[] readers = new LongValues[meta.numBlocks];
    long offset = 0L;
    for (int i = 0; i < meta.numBlocks; ++i) {
      byte bpvs = meta.bpvs[i];
      if (bpvs == 0) {
        readers[i] = LongValues.ZEROES;
      } else if (merging
          && i < meta.numBlocks - 1 // we only know the number of values for the last block
          && meta.blockShift >= DirectReader.MERGE_BUFFER_SHIFT) {
        readers[i] = DirectReader.getMergeInstance(data, bpvs, offset, 1L << meta.blockShift);
      } else {
        readers[i] = DirectReader.getInstance(data, bpvs, offset);
      }
      if (bpvs > 0) {
        int paddingBitsNeeded;
        if (bpvs > Integer.SIZE) {
          paddingBitsNeeded = Long.SIZE - bpvs;
        } else if (bpvs > Short.SIZE) {
          paddingBitsNeeded = Integer.SIZE - bpvs;
        } else if (bpvs > Byte.SIZE) {
          paddingBitsNeeded = Short.SIZE - bpvs;
        } else {
          paddingBitsNeeded = 0;
        }
        offset += (((1L << meta.blockShift) * bpvs) + paddingBitsNeeded + 7) / 8;
      }
    }

    return new DirectMonotonicReader(meta.blockShift, readers, meta.mins, meta.avgs, meta.bpvs);
  }

  public static LongValues getLongValues(Meta meta, RandomAccessInput data) {
    return getLongValues(meta, data, false);
  }

  public static LongValues getLongValues(Meta meta, RandomAccessInput data, boolean merging) {
    if (meta == Meta.SINGLE_ZERO_BLOCK) {
      return ZEROES;
    }
    boolean allValuesZero = true;
    for (byte bpv : meta.bpvs) {
      if (bpv != 0) {
        allValuesZero = false;
        break;
      }
    }
    if (allValuesZero) {
      if (meta.numBlocks == 1) {
        return LongValues.linear(meta.avgs[0], meta.mins[0]);
      } else {
        long[] mins = meta.mins;
        float[] avgs = meta.avgs;
        int blockShift = meta.blockShift;
        long blockMask = (1L << blockShift) - 1;
        return new LongValues() {
          @Override
          public long get(long index) {
            final int block = (int) (index >>> blockShift);
            final long blockIndex = index & blockMask;
            return mins[block] + (long) (avgs[block] * blockIndex);
          }
        };
      }
    }
    if (meta.numBlocks == 1) {
      var reader = DirectReader.getInstance(data, meta.bpvs[0], 0);
      long min = meta.mins[0];
      float avg = meta.avgs[0];
      return new LongValues() {
        @Override
        public long get(long index) {
          return min + (long) (avg * index) + reader.get(index);
        }
      };
    }
    return getInstance(meta, data, merging);
  }

  private final int blockShift;
  private final long blockMask;
  private final LongValues[] readers;
  private final long[] mins;
  private final float[] avgs;
  private final byte[] bpvs;

  private DirectMonotonicReader(
      int blockShift, LongValues[] readers, long[] mins, float[] avgs, byte[] bpvs) {
    this.blockShift = blockShift;
    this.blockMask = (1L << blockShift) - 1;
    this.readers = readers;
    this.mins = mins;
    this.avgs = avgs;
    this.bpvs = bpvs;
    if (readers.length != mins.length
        || readers.length != avgs.length
        || readers.length != bpvs.length) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public long get(long index) {
    final int block = (int) (index >>> blockShift);
    final long blockIndex = index & blockMask;
    final long delta = readers[block].get(blockIndex);
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
