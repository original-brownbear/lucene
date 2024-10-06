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
      };

  public static LongValues linear(float slope, long intercept) {
    if (slope == 0f) {
      return ofConstant(intercept);
    }
    if (intercept == 0L && slope == 1f) {
      return IDENTITY.add(intercept);
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return (long) (slope * index) + intercept;
      }
    };
  }

  public static LongValues ofConstant(long value) {
    if (value == 0) {
      return ZEROES;
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return value;
      }

      @Override
      public LongValues add(long added) {
        if (added == 0) {
          return this;
        }
        return ofConstant(value + added);
      }
    };
  }

  public LongValues add(long shift) {
    if (shift == 0) {
      return this;
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return LongValues.this.get(index) + shift;
      }

      @Override
      public LongValues add(long added) {
        if (added == 0) {
          return this;
        }
        return LongValues.this.add(shift + added);
      }
    };
  }

  /** Get value at <code>index</code>. */
  public abstract long get(long index);
}
