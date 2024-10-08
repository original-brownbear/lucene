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
package org.apache.lucene.util;

/**
 * Abstraction over an array of longs.
 *
 * @lucene.internal
 */
public abstract class LongValues {

  /** An instance that returns the provided value. */
  public static final LongValues IDENTITY =
      new LongValues() {

        @Override
        public long get(long index) {
          return index;
        }

        @Override
        public LongValues linearTransform(long factor, long yShift) {
          return linear(factor, yShift);
        }
      };

  public static final LongValues ZEROES =
      new LongValues() {

        @Override
        public long get(long index) {
          return 0;
        }

        @Override
        public LongValues linearTransform(long factor, long yShift) {
          return constant(yShift);
        }
      };

  public static LongValues constant(long value) {
    if (value == 0) {
      return ZEROES;
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return value;
      }

      @Override
      public LongValues linearTransform(long factor, long yShift) {
        return constant(factor * value + yShift);
      }
    };
  }

  public static LongValues linear(long slope) {
    if (slope == 1) {
      return IDENTITY;
    }
    return new LongValues() {

      @Override
      public long get(long index) {
        return slope * index;
      }

      @Override
      public LongValues linearTransform(long factor, long yShift) {
        return linear(factor * slope, yShift);
      }
    };
  }

  public static LongValues linear(long slope, long intersect) {
    if (slope == 0L) {
      return constant(intersect);
    }
    if (intersect == 0) {
      return linear(slope);
    }
    return new LongValues() {

      @Override
      public long get(long index) {
        return slope * index + intersect;
      }

      @Override
      public LongValues linearTransform(long factor, long yShift) {
        return linear(factor * slope, factor * intersect + yShift);
      }
    };
  }

  public static LongValues linear(float slope, long intersect) {
    if (slope == 0L) {
      return constant(intersect);
    }
    return new LongValues() {

      @Override
      public long get(long index) {
        return (long) (slope * index) + intersect;
      }
    };
  }

  /** Get value at <code>index</code>. */
  public abstract long get(long index);

  public final LongValues add(long value) {
    if (value == 0) {
      return this;
    }
    return linearTransform(1, value);
  }

  public LongValues linearTransform(long factor, long yShift) {
    return linearTransform(this, factor, yShift);
  }

  private static LongValues linearTransform(LongValues values, long factor, long yShift) {
    return new LongValues() {

      @Override
      public long get(long index) {
        return values.get(index) * factor + yShift;
      }

      @Override
      public LongValues linearTransform(long f, long s) {
        return values.linearTransform(factor * f, f * yShift + s);
      }
    };
  }
}
