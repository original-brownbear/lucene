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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;

/**
 * Compute facets counts from previously indexed {@link SortedSetDocValuesFacetField}, without
 * require a separate taxonomy index. Faceting is a bit slower (~25%), and there is added cost on
 * every {@link IndexReader} open to create a new {@link SortedSetDocValuesReaderState}.
 *
 * <p><b>NOTE</b>: this class should be instantiated and then used from a single thread, because it
 * holds a thread-private instance of {@link SortedSetDocValues}.
 *
 * <p><b>NOTE</b>: tie-break is by unicode sort order
 *
 * <p><b>NOTE</b>: if you have multi-valued dims that require dim counts (see {@link FacetsConfig},
 * make sure to provide your {@code FacetsConfig} instance when instantiating {@link
 * SortedSetDocValuesReaderState}, or else dim counts can be inaccurate
 *
 * @lucene.experimental
 */
public class SortedSetDocValuesFacetCounts extends AbstractSortedSetDocValueFacetCounts {
  private final SortedSetDocValuesReaderState state;
  int[] counts;

  /** Returns all facet counts, same result as searching on {@link MatchAllDocsQuery} but faster. */
  public SortedSetDocValuesFacetCounts(SortedSetDocValuesReaderState state) throws IOException {
    this(state, null);
  }

  /** Counts all facet dimensions across the provided hits. */
  public SortedSetDocValuesFacetCounts(SortedSetDocValuesReaderState state, FacetsCollector hits)
      throws IOException {
    super(state);
    this.state = state;
    if (hits == null) {
      // browse only
      countAll();
    } else {
      count(hits.getMatchingDocs());
    }
  }

  private void initializeCounts() {
    if (counts == null) {
      counts = new int[state.getSize()];
    }
  }

  @Override
  boolean hasCounts() {
    return counts != null;
  }

  @Override
  int getCount(int ord) {
    return counts[ord];
  }

  // Variant of countOneSegment, that has No Hits or Live Docs
  private void countOneSegmentNHLDNoOrdinalMap(LeafReader reader) throws IOException {
    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, field);
    if (multiValues == null) {
      // nothing to count
      return;
    }

    // Initialize counts:
    initializeCounts();

    // No ord mapping (e.g., single segment index):
    // just aggregate directly into counts:
    countNHDL(counts, multiValues);
  }

  // Variant of countOneSegment, that has No Hits or Live Docs
  private void countOneSegmentNHLD(OrdinalMap ordinalMap, LeafReader reader, int segOrd)
      throws IOException {
    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, field);
    if (multiValues == null) {
      // nothing to count
      return;
    }

    // Initialize counts:
    initializeCounts();

    // TODO: yet another option is to count all segs
    // first, only in seg-ord space, and then do a
    // merge-sort-PQ in the end to only "resolve to
    // global" those seg ords that can compete, if we know
    // we just want top K?  ie, this is the same algo
    // that'd be used for merging facets across shards
    // (distributed faceting).  but this has much higher
    // temp ram req'ts (sum of number of ords across all
    // segs)
    final LongValues ordMap;
    if (ordinalMap != null && (ordMap = ordinalMap.getGlobalOrds(segOrd)) != LongValues.IDENTITY) {

      // First count in seg-ord space:
      final int[] segCounts = new int[(int) multiValues.getValueCount()];
      countNHDL(segCounts, multiValues);

      // Then, migrate to global ords:
      migrateToGlobalOrds(segCounts, counts, ordMap);
    } else {
      // No ord mapping (e.g., single segment index):
      // just aggregate directly into counts:
      countNHDL(counts, multiValues);
    }
  }

  private static void migrateToGlobalOrds(int[] segCounts, int[] globalCounts, LongValues ordMap) {
    for (int ord = 0; ord < segCounts.length; ord++) {
      int count = segCounts[ord];
      if (count != 0) {
        // ordinalMap.getGlobalOrd(segOrd, ord));
        globalCounts[(int) ordMap.get(ord)] += count;
      }
    }
  }

  private static void countNHDL(int[] segCounts, SortedSetDocValues multiValues)
      throws IOException {
    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
    if (singleValues != null) {
      countSingleValues(singleValues, segCounts);
    } else {
      countMultiValues(multiValues, segCounts);
    }
  }

  private void countOneSegmentNoOrdinalMap(LeafReader reader, Bits liveDocs) throws IOException {
    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, field);
    if (multiValues == null) {
      // nothing to count
      return;
    }

    // Initialize counts:
    initializeCounts();

    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
    DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;

    assert liveDocs != null;
    DocIdSetIterator it = FacetUtils.liveDocsDISI(valuesIt, liveDocs);
    // No ord mapping (e.g., single segment index):
    // just aggregate directly into counts:
    countWithoutOrdMapping(singleValues, it, multiValues, counts);
  }

  private void countOneSegment(
      OrdinalMap ordinalMap, LeafReader reader, int segOrd, MatchingDocs hits, Bits liveDocs)
      throws IOException {
    if (hits != null && hits.totalHits == 0) {
      return;
    }

    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, field);
    if (multiValues == null) {
      // nothing to count
      return;
    }

    // Initialize counts:
    initializeCounts();

    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
    DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;

    DocIdSetIterator it;
    if (hits == null) {
      assert liveDocs != null;
      it = FacetUtils.liveDocsDISI(valuesIt, liveDocs);
    } else {
      it = ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), valuesIt));
    }

    // TODO: yet another option is to count all segs
    // first, only in seg-ord space, and then do a
    // merge-sort-PQ in the end to only "resolve to
    // global" those seg ords that can compete, if we know
    // we just want top K?  ie, this is the same algo
    // that'd be used for merging facets across shards
    // (distributed faceting).  but this has much higher
    // temp ram req'ts (sum of number of ords across all
    // segs)
    final LongValues ordMap;
    if (ordinalMap != null && (ordMap = ordinalMap.getGlobalOrds(segOrd)) != LongValues.IDENTITY) {

      int numSegOrds = (int) multiValues.getValueCount();

      if (hits != null && hits.totalHits < numSegOrds / 10) {
        // Remap every ord to global ord as we iterate:
        if (singleValues != null) {
          countSingleWithOrdMap(it, singleValues, ordMap, counts);
        } else {
          countMultiWithOrdMap(it, multiValues, ordMap, counts);
        }
      } else {
        // First count in seg-ord space:
        final int[] segCounts = new int[numSegOrds];
        countWithoutOrdMapping(singleValues, it, multiValues, segCounts);

        // Then, migrate to global ords:
        migrateToGlobalOrds(segCounts, counts, ordMap);
      }
    } else {
      // No ord mapping (e.g., single segment index):
      // just aggregate directly into counts:
      countWithoutOrdMapping(singleValues, it, multiValues, counts);
    }
  }

  private static void countWithoutOrdMapping(
      SortedDocValues singleValues,
      DocIdSetIterator it,
      SortedSetDocValues multiValues,
      int[] counts)
      throws IOException {
    if (singleValues != null) {
      countSingleValues(it, singleValues, counts);
    } else {
      countMultiValues(it, multiValues, counts);
    }
  }

  private static void countSingleWithOrdMap(
      DocIdSetIterator it, SortedDocValues singleValues, LongValues ordMap, int[] counts)
      throws IOException {
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      counts[(int) ordMap.get(singleValues.ordValue())]++;
    }
  }

  private static void countMultiWithOrdMap(
      DocIdSetIterator it, SortedSetDocValues multiValues, LongValues ordMap, int[] counts)
      throws IOException {
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      final int docValueCount = multiValues.docValueCount();
      for (int i = 0; i < docValueCount; i++) {
        int term = (int) multiValues.nextOrd();
        counts[(int) ordMap.get(term)]++;
      }
    }
  }

  private static void countSingleValues(SortedDocValues singleValues, int[] counts)
      throws IOException {
    while (singleValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      counts[singleValues.ordValue()]++;
    }
  }

  private static void countSingleValues(
      DocIdSetIterator it, SortedDocValues singleValues, int[] counts) throws IOException {
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      counts[singleValues.ordValue()]++;
    }
  }

  private static void countMultiValues(SortedSetDocValues multiValues, int[] counts)
      throws IOException {
    while (multiValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      final int docValueCount = multiValues.docValueCount();
      for (int i = 0; i < docValueCount; i++) {
        int term = (int) multiValues.nextOrd();
        counts[term]++;
      }
    }
  }

  private static void countMultiValues(
      DocIdSetIterator it, SortedSetDocValues multiValues, int[] counts) throws IOException {
    while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      final int docValueCount = multiValues.docValueCount();
      for (int i = 0; i < docValueCount; i++) {
        int term = (int) multiValues.nextOrd();
        counts[term]++;
      }
    }
  }

  /** Does all the "real work" of tallying up the counts. */
  private void count(List<MatchingDocs> matchingDocs) throws IOException {

    OrdinalMap ordinalMap;

    // TODO: is this right?  really, we need a way to
    // verify that this ordinalMap "matches" the leaves in
    // matchingDocs...
    if (dv instanceof MultiDocValues.MultiSortedSetDocValues && matchingDocs.size() > 1) {
      ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
    } else {
      ordinalMap = null;
    }

    IndexReader reader = state.getReader();

    for (MatchingDocs hits : matchingDocs) {

      var context = hits.context;
      // LUCENE-5090: make sure the provided reader context "matches"
      // the top-level reader passed to the
      // SortedSetDocValuesReaderState, else cryptic
      // AIOOBE can happen:
      if (ReaderUtil.getTopLevelContext(context).reader() != reader) {
        throw new IllegalStateException(
            "the SortedSetDocValuesReaderState provided to this class does not match the reader being searched; you must create a new SortedSetDocValuesReaderState every time you open a new IndexReader");
      }

      countOneSegment(ordinalMap, context.reader(), context.ord, hits, null);
    }
  }

  /** Does all the "real work" of tallying up the counts. */
  private void countAll() throws IOException {
    // TODO: is this right?  really, we need a way to
    // verify that this ordinalMap "matches" the leaves in
    // matchingDocs...
    if (dv instanceof MultiDocValues.MultiSortedSetDocValues multiSortedSetDocValues) {
      OrdinalMap ordinalMap = multiSortedSetDocValues.mapping;
      for (LeafReaderContext context : state.getReader().leaves()) {
        var reader = context.reader();
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
          countOneSegmentNHLD(ordinalMap, reader, context.ord);
        } else {
          countOneSegment(ordinalMap, reader, context.ord, null, liveDocs);
        }
      }
    } else {
      for (LeafReaderContext context : state.getReader().leaves()) {
        var reader = context.reader();
        Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
          countOneSegmentNHLDNoOrdinalMap(reader);
        } else {
          countOneSegmentNoOrdinalMap(reader, liveDocs);
        }
      }
    }
  }
}
