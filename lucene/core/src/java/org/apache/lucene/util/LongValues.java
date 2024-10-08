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
        public LongValues add(long shift) {
          return linear(1, shift);
        }

        @Override
        public LongValues addLinear(float slope, long intercept) {
          return linear(slope + 1, intercept);
        }

        @Override
        public LongValues stretchByFloat(float factor) {
          return multiplyByFloat(factor);
        }

        @Override
        public LongValues stretchByLong(long factor) {
          return super.stretchByLong(factor);
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

        @Override
        public LongValues stretchByFloat(float factor) {
          return ZEROES;
        }

        @Override
        public LongValues addLinear(float slope, long intercept) {
          return linear(slope, intercept);
        }

        @Override
        public LongValues transformLinear(long factor, long shift) {
          return constant(shift);
        }
      };

  public static LongValues linear(float slope, long intercept) {
    if (slope == 0) {
      return constant(intercept);
    }
    if (slope == 1) {
      if (intercept == 0) {
        return IDENTITY;
      }
      return new LongValues() {
        @Override
        public long get(long index) {
          return index + intercept;
        }

        @Override
        public LongValues add(long shift) {
          return linear(1, shift + intercept);
        }

        @Override
        public LongValues addLinear(float slope, long i) {
          return linear(slope + 1, intercept + i);
        }
      };
    }
    if (intercept == 0) {
      return multiplyByFloat(slope);
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

  public static LongValues multiplyByFloat(float factor) {
    return new LongValues() {

      @Override
      public long get(long index) {
        return (long) (factor * index);
      }

      @Override
      public LongValues add(long shift) {
        return linear(factor, shift);
      }

      @Override
      public LongValues addLinear(float slope, long intercept) {
        return linear(factor + slope, intercept);
      }

      @Override
      public LongValues stretchByFloat(float f) {
        return multiplyByFloat(f * factor);
      }

      @Override
      public LongValues stretchByLong(long f) {
        return multiplyByFloat(factor * f);
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

  public LongValues addLinear(float slope, long intercept) {
    if (slope == 0) {
      return add(intercept);
    }
    return addLinear(this, slope, intercept);
  }

  public LongValues transformLinear(long factor, long shift) {
    return transformLinear(this, factor, shift);
  }

  private static LongValues transformLinear(LongValues values, long factor, long shift) {
    if (factor == 0) {
      return constant(shift);
    }
    if (factor == 1) {
      return values.add(shift);
    }
    if (shift == 0) {
      return values.stretchByLong(factor);
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return (factor * values.get(index)) + shift;
      }

      @Override
      public LongValues add(long s) {
        return values.transformLinear(factor, shift + s);
      }

      @Override
      public LongValues stretchByLong(long f) {
        return values.stretchByLong(factor * f).add(f * shift);
      }
    };
  }

  public LongValues stretchByFloat(float factor) {
    return stretchedByFloat(this, factor);
  }

  public LongValues stretchByLong(long factor) {
    return stretchedByLong(this, factor);
  }

  private static LongValues stretchedByFloat(LongValues values, float factor) {
    if (factor == 1) {
      return values;
    }
    if (factor == 0) {
      return ZEROES;
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return (long) (factor * values.get(index));
      }

      @Override
      public LongValues stretchByFloat(float f) {
        return values.stretchByFloat(factor * f);
      }

      @Override
      public LongValues stretchByLong(long f) {
        return values.stretchByFloat(factor * f);
      }
    };
  }

  private static LongValues stretchedByLong(LongValues values, long factor) {
    if (factor == 1) {
      return values;
    }
    if (factor == 0) {
      return ZEROES;
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return factor * values.get(index);
      }

      @Override
      public LongValues stretchByFloat(float f) {
        return values.stretchByFloat(factor * f);
      }

      @Override
      public LongValues stretchByLong(long f) {
        return values.stretchByLong(factor * f);
      }
    };
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
        return values.add(added + shift);
      }
    };
  }

  private static LongValues addLinear(LongValues values, float slope, long intercept) {
    if (slope == 0) {
      return values.add(intercept);
    }
    if (intercept == 0) {
      return new LongValues() {
        @Override
        public long get(long index) {
          return values.get(index) + (long) (slope * index);
        }

        @Override
        public LongValues add(long shift) {
          return values.addLinear(slope, shift);
        }

        @Override
        public LongValues addLinear(float s, long i) {
          return values.addLinear(slope + s, i);
        }
      };
    }
    return new LongValues() {
      @Override
      public long get(long index) {
        return values.get(index) + (long) (slope * index) + intercept;
      }

      @Override
      public LongValues add(long shift) {
        return values.addLinear(slope, shift + intercept);
      }

      @Override
      public LongValues addLinear(float s, long i) {
        return values.addLinear(slope + s, i + intercept);
      }
    };
  }
}
