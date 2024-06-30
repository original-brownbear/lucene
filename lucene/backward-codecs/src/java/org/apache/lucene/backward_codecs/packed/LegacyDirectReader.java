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
package org.apache.lucene.backward_codecs.packed;

import java.io.IOException;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link LegacyDirectWriter}
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 *   int bitsPerValue = 100;
 *   IndexInput in = dir.openInput("packed", IOContext.DEFAULT);
 *   LongValues values = LegacyDirectReader.getInstance(in.randomAccessSlice(start, end), bitsPerValue);
 *   for (int i = 0; i &lt; numValues; i++) {
 *     long value = values.get(i);
 *   }
 * </pre>
 *
 * @see LegacyDirectWriter
 */
public class LegacyDirectReader {

  private LegacyDirectReader() {
    // no instances
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
  public static LongValues getInstance(RandomAccessInput slice, int bitsPerValue, long offset) {
    return switch (bitsPerValue) {
      case 1 -> new DirectPackedReader1(slice, offset);
      case 2 -> new DirectPackedReader2(slice, offset);
      case 4 -> new DirectPackedReader4(slice, offset);
      case 8 -> new DirectPackedReader8(slice, offset);
      case 12 -> new DirectPackedReader12(slice, offset);
      case 16 -> new DirectPackedReader16(slice, offset);
      case 20 -> new DirectPackedReader20(slice, offset);
      case 24 -> new DirectPackedReader24(slice, offset);
      case 28 -> new DirectPackedReader28(slice, offset);
      case 32 -> new DirectPackedReader32(slice, offset);
      case 40 -> new DirectPackedReader40(slice, offset);
      case 48 -> new DirectPackedReader48(slice, offset);
      case 56 -> new DirectPackedReader56(slice, offset);
      case 64 -> new DirectPackedReader64(slice, offset);
      default -> throw new IllegalArgumentException("unsupported bitsPerValue: " + bitsPerValue);
    };
  }

  static final class DirectPackedReader1 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader1(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        int shift = 7 - (int) (index & 7);
        return (in.readByte(offset + (index >>> 3)) >>> shift) & 0x1;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader2 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader2(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        int shift = (3 - (int) (index & 3)) << 1;
        return (in.readByte(offset + (index >>> 2)) >>> shift) & 0x3;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader4 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader4(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        int shift = (int) ((index + 1) & 1) << 2;
        return (in.readByte(offset + (index >>> 1)) >>> shift) & 0xF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader8 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader8(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readByte(offset + index) & 0xFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader12 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader12(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        long offset = (index * 12) >>> 3;
        int shift = (int) ((index + 1) & 1) << 2;
        return (in.readShort(this.offset + offset) >>> shift) & 0xFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader16 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader16(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readShort(offset + (index << 1)) & 0xFFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader20 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader20(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        long offset = (index * 20) >>> 3;
        // TODO: clean this up...
        int v = in.readInt(this.offset + offset) >>> 8;
        int shift = (int) ((index + 1) & 1) << 2;
        return (v >>> shift) & 0xFFFFF;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader24 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader24(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readInt(offset + index * 3) >>> 8;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader28 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader28(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        long offset = (index * 28) >>> 3;
        int shift = (int) ((index + 1) & 1) << 2;
        return (in.readInt(this.offset + offset) >>> shift) & 0xFFFFFFFL;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader32 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader32(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readInt(this.offset + (index << 2)) & 0xFFFFFFFFL;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader40 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader40(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readLong(this.offset + index * 5) >>> 24;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader48 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader48(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readLong(this.offset + index * 6) >>> 16;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader56 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader56(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readLong(this.offset + index * 7) >>> 8;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class DirectPackedReader64 implements LongValues {
    final RandomAccessInput in;
    final long offset;

    DirectPackedReader64(RandomAccessInput in, long offset) {
      this.in = in;
      this.offset = offset;
    }

    @Override
    public long get(long index) {
      try {
        return in.readLong(offset + (index << 3));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
