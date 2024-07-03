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
import java.io.UncheckedIOException;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link DirectWriter}
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = DirectWriter.bitsRequired(100);
 *   IndexInput in = dir.openInput("packed", IOContext.DEFAULT);
 *   LongValues values = DirectReader.getInstance(in.randomAccessSlice(start, end), bitsPerValue);
 *   for (int i = 0; i &lt; numValues; i++) {
 *     long value = values.get(i);
 *   }
 * </pre>
 *
 * @see DirectWriter
 */
public class DirectReader {

  static final int MERGE_BUFFER_SHIFT = 7;
  private static final int MERGE_BUFFER_SIZE = 1 << MERGE_BUFFER_SHIFT;
  private static final int MERGE_BUFFER_MASK = MERGE_BUFFER_SIZE - 1;

  /** Reads a long from the given input at given offset and block index. */
  public abstract static class DirectReadFunction {

    public final byte bitsPerValue;

    protected DirectReadFunction(byte bitsPerValue) {
      this.bitsPerValue = bitsPerValue;
    }

    public abstract long read(RandomAccessInput slice, long offset, long index) throws IOException;
  }

  private static final DirectReadFunction READ_0 =
      new DirectReadFunction((byte) 0) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) {
          return 0;
        }
      };

  private static final DirectReadFunction READ_1 =
      new DirectReadFunction((byte) 1) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read1(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_2 =
      new DirectReadFunction((byte) 2) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read2(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_4 =
      new DirectReadFunction((byte) 4) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read4(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_8 =
      new DirectReadFunction((byte) 8) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read8(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_12 =
      new DirectReadFunction((byte) 12) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read12(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_16 =
      new DirectReadFunction((byte) 16) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read16(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_20 =
      new DirectReadFunction((byte) 20) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read20(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_24 =
      new DirectReadFunction((byte) 24) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read24(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_28 =
      new DirectReadFunction((byte) 28) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read28(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_32 =
      new DirectReadFunction((byte) 32) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read32(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_64 =
      new DirectReadFunction((byte) 64) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read64(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_56 =
      new DirectReadFunction((byte) 56) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read56(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_48 =
      new DirectReadFunction((byte) 48) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read48(slice, offset, index);
        }
      };

  private static final DirectReadFunction READ_40 =
      new DirectReadFunction((byte) 40) {
        @Override
        public long read(RandomAccessInput slice, long offset, long index) throws IOException {
          return read40(slice, offset, index);
        }
      };

  public static DirectReadFunction readFunction(int bitsPerValue) {
    return switch (bitsPerValue) {
      case 0 -> READ_0;
      case 1 -> READ_1;
      case 2 -> READ_2;
      case 4 -> READ_4;
      case 8 -> READ_8;
      case 12 -> READ_12;
      case 16 -> READ_16;
      case 20 -> READ_20;
      case 24 -> READ_24;
      case 28 -> READ_28;
      case 32 -> READ_32;
      case 40 -> READ_40;
      case 48 -> READ_48;
      case 56 -> READ_56;
      case 64 -> READ_64;
      default -> throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    };
  }

  private static long read64(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readLong(offset + (index << 3));
  }

  private static long read56(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readLong(offset + index * 7) & 0xFFFFFFFFFFFFFFL;
  }

  private static long read48(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readLong(offset + index * 6) & 0xFFFFFFFFFFFFL;
  }

  private static long read40(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readLong(offset + index * 5) & 0xFFFFFFFFFFL;
  }

  private static long read32(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readInt(offset + (index << 2)) & 0xFFFFFFFFL;
  }

  private static long read28(RandomAccessInput slice, long offset, long index) throws IOException {
    return (slice.readInt(offset + ((index * 28) >>> 3)) >>> ((int) (index & 1) << 2)) & 0xFFFFFFF;
  }

  private static long read24(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readInt(offset + index * 3) & 0xFFFFFF;
  }

  private static long read20(RandomAccessInput slice, long offset, long index) throws IOException {
    return (slice.readInt(offset + ((index * 20) >>> 3)) >>> ((int) (index & 1) << 2)) & 0xFFFFF;
  }

  private static long read16(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readShort(offset + (index << 1)) & 0xFFFF;
  }

  private static long read12(RandomAccessInput slice, long offset, long index) throws IOException {
    return (slice.readShort(offset + ((index * 12) >>> 3)) >>> ((int) (index & 1) << 2)) & 0xFFF;
  }

  private static long read8(RandomAccessInput slice, long offset, long index) throws IOException {
    return slice.readByte(offset + index) & 0xFF;
  }

  private static long read4(RandomAccessInput slice, long offset, long index) throws IOException {
    return (slice.readByte(offset + (index >>> 1)) >>> ((int) (index & 1) << 2)) & 0xF;
  }

  private static long read2(RandomAccessInput slice, long offset, long index) throws IOException {
    return (slice.readByte(offset + (index >>> 2)) >>> (((int) (index & 3)) << 1)) & 0x3;
  }

  private static long read1(RandomAccessInput slice, long offset, long index) throws IOException {
    return (slice.readByte(offset + (index >>> 3)) >>> (int) (index & 7)) & 0x1;
  }

  /**
   * Retrieves an instance from the specified slice written decoding {@code bitsPerValue} for each
   * value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue) {
    return getInstance(slice, bitsPerValue, 0);
  }

  /**
   * Retrieves an instance from the specified {@code offset} of the given slice decoding {@code
   * bitsPerValue} for each value
   */
  /**
   * Retrieves an instance from the specified {@code offset} of the given slice decoding {@code
   * bitsPerValue} for each value
   */
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue, long offset) {
    switch (bitsPerValue) {
      case 1:
        return new DirectPackedReader1(slice, offset);
      case 2:
        return new DirectPackedReader2(slice, offset);
      case 4:
        return new DirectPackedReader4(slice, offset);
      case 8:
        return new DirectPackedReader8(slice, offset);
      case 12:
        return new DirectPackedReader12(slice, offset);
      case 16:
        return new DirectPackedReader16(slice, offset);
      case 20:
        return new DirectPackedReader20(slice, offset);
      case 24:
        return new DirectPackedReader24(slice, offset);
      case 28:
        return new DirectPackedReader28(slice, offset);
      case 32:
        return new DirectPackedReader32(slice, offset);
      case 40:
        return new DirectPackedReader40(slice, offset);
      case 48:
        return new DirectPackedReader48(slice, offset);
      case 56:
        return new DirectPackedReader56(slice, offset);
      case 64:
        return new DirectPackedReader64(slice, offset);
      default:
        throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    }
  }

  /**
   * Retrieves an instance that is specialized for merges and is typically faster at sequential
   * access but slower at random access.
   */
  public static LongValues getMergeInstance(
      RandomAccessInput slice, int bitsPerValue, long numValues) {
    return getMergeInstance(slice, bitsPerValue, 0L, numValues);
  }

  /**
   * Retrieves an instance that is specialized for merges and is typically faster at sequential
   * access.
   */
  public static LongValues getMergeInstance(
      RandomAccessInput slice, int bitsPerValue, long baseOffset, long numValues) {
    return new LongValues() {

      private final long[] buffer = new long[MERGE_BUFFER_SIZE];
      private long blockIndex = -1;

      @Override
      public long get(long index) {
        assert index < numValues;
        final long blockIndex = index >>> MERGE_BUFFER_SHIFT;
        if (this.blockIndex != blockIndex) {
          try {
            fillBuffer(blockIndex << MERGE_BUFFER_SHIFT);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          this.blockIndex = blockIndex;
        }
        return buffer[(int) (index & MERGE_BUFFER_MASK)];
      }

      private void fillBuffer(long index) throws IOException {
        // NOTE: we're not allowed to read more than 3 bytes past the last value
        if (index >= numValues - MERGE_BUFFER_SIZE) {
          // 128 values left or less
          final LongValues slowInstance = getInstance(slice, bitsPerValue, baseOffset);
          final int numValuesLastBlock = Math.toIntExact(numValues - index);
          for (int i = 0; i < numValuesLastBlock; ++i) {
            buffer[i] = slowInstance.get(index + i);
          }
        } else if ((bitsPerValue & 0x07) == 0) {
          // bitsPerValue is a multiple of 8: 8, 16, 24, 32, 30, 48, 56, 64
          final int bytesPerValue = bitsPerValue / Byte.SIZE;
          final long mask = bitsPerValue == 64 ? ~0L : (1L << bitsPerValue) - 1;
          long offset = baseOffset + (index * bitsPerValue) / 8;
          for (int i = 0; i < MERGE_BUFFER_SIZE; ++i) {
            if (bitsPerValue > Integer.SIZE) {
              buffer[i] = slice.readLong(offset) & mask;
            } else if (bitsPerValue > Short.SIZE) {
              buffer[i] = slice.readInt(offset) & mask;
            } else if (bitsPerValue > Byte.SIZE) {
              buffer[i] = Short.toUnsignedLong(slice.readShort(offset));
            } else {
              buffer[i] = Byte.toUnsignedLong(slice.readByte(offset));
            }
            offset += bytesPerValue;
          }
        } else if (bitsPerValue < 8) {
          // bitsPerValue is 1, 2 or 4
          final int valuesPerLong = Long.SIZE / bitsPerValue;
          final long mask = (1L << bitsPerValue) - 1;
          long offset = baseOffset + (index * bitsPerValue) / 8;
          int i = 0;
          for (int l = 0; l < 2 * bitsPerValue; ++l) {
            final long bits = slice.readLong(offset);
            for (int j = 0; j < valuesPerLong; ++j) {
              buffer[i++] = (bits >>> (j * bitsPerValue)) & mask;
            }
            offset += Long.BYTES;
          }
        } else {
          // bitsPerValue is 12, 20 or 28
          // Read values 2 by 2
          final int numBytesFor2Values = bitsPerValue * 2 / Byte.SIZE;
          final long mask = (1L << bitsPerValue) - 1;
          long offset = baseOffset + (index * bitsPerValue) / 8;
          for (int i = 0; i < MERGE_BUFFER_SIZE; i += 2) {
            final long l;
            if (numBytesFor2Values > Integer.BYTES) {
              l = slice.readLong(offset);
            } else {
              l = slice.readInt(offset);
            }
            buffer[i] = l & mask;
            buffer[i + 1] = (l >>> bitsPerValue) & mask;
            offset += numBytesFor2Values;
          }
        }
      }
    };
  }

  static final class DirectPackedReader1 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader1(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read1(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader2 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader2(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read2(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader4 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader4(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read4(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader8 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader8(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read8(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader12 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader12(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read12(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader16 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader16(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read16(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader20 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader20(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read20(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader24 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader24(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read24(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader28 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader28(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read28(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader32 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader32(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read32(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader40 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader40(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read40(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader48 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader48(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read48(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader56 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader56(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read56(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader64 extends LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader64(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return read64(in, offset, index);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
