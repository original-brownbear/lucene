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
      };

  public static final LongValues ZEROES =
      new LongValues() {

        @Override
        public long get(long index) {
          return 0;
        }

        @Override
        public LongValues add(long shift) {
          return shift == 0 ? ZEROES : constant(shift);
        }
      };

  public static LongValues linear(float slope, long intercept) {
    if (slope == 0) {
      return constant(intercept);
    }
    if (slope == 1 && intercept == 0) {
      return IDENTITY;
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return (long) (slope * index) + intercept;
      }

      @Override
      public LongValues add(long shift) {
        return linear(slope, shift + intercept);
      }
    };
  }

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
      public LongValues add(long shift) {
        return constant(shift + value);
      }
    };
  }

  /** Get value at <code>index</code>. */
  public abstract long get(long index);

  public LongValues add(long shift) {
    return shifted(this, shift);
  }

  private static LongValues shifted(LongValues values, long added) {
    if (added == 0) {
      return values;
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return values.get(index) + added;
      }

      @Override
      public LongValues add(long shift) {
        return shifted(values, added + shift);
      }
    };
  }
}
