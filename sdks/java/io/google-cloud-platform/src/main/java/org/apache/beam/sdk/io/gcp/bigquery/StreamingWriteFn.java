/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.*;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;

/** Implementation of DoFn to perform streaming BigQuery write. */
@SystemDoFnInternal
@VisibleForTesting
class StreamingWriteFn<ErrorT, ElementT>
    extends DoFn<KV<ShardedKey<String>, TableRowInfo<ElementT>>, Void> {
  private final BigQueryServices bqServices;
  private final InsertRetryPolicy retryPolicy;
  private final TupleTag<ErrorT> failedOutputTag;
  private final ErrorContainer<ErrorT> errorContainer;
  private final boolean skipInvalidRows;
  private final boolean ignoreUnknownValues;
  private final SerializableFunction<ElementT, TableRow> toTableRow;

  /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
  private transient Map<String, List<ValueInSingleWindow<TableRow>>> tableRows;

  /** The list of unique ids for each BigQuery table row. */
  private transient Map<String, List<String>> uniqueIdsForTableRows;

  private transient OpTracker tracker = new OpTracker();

  StreamingWriteFn(
      BigQueryServices bqServices,
      InsertRetryPolicy retryPolicy,
      TupleTag<ErrorT> failedOutputTag,
      ErrorContainer<ErrorT> errorContainer,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      SerializableFunction<ElementT, TableRow> toTableRow) {
    this.bqServices = bqServices;
    this.retryPolicy = retryPolicy;
    this.failedOutputTag = failedOutputTag;
    this.errorContainer = errorContainer;
    this.skipInvalidRows = skipInvalidRows;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.toTableRow = toTableRow;
  }

  /** Prepares a target BigQuery table. */
  @StartBundle
  public void startBundle() {
    tableRows = new HashMap<>();
    uniqueIdsForTableRows = new HashMap<>();
  }

  /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
  @ProcessElement
  public void processElement(
      @Element KV<ShardedKey<String>, TableRowInfo<ElementT>> element,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo pane) {
    String tableSpec = element.getKey().getKey();
    List<ValueInSingleWindow<TableRow>> rows =
        BigQueryHelpers.getOrCreateMapListValue(tableRows, tableSpec);
    List<String> uniqueIds =
        BigQueryHelpers.getOrCreateMapListValue(uniqueIdsForTableRows, tableSpec);

    TableRow tableRow = toTableRow.apply(element.getValue().tableRow);
    rows.add(ValueInSingleWindow.of(tableRow, timestamp, window, pane));
    uniqueIds.add(element.getValue().uniqueId);
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    List<ValueInSingleWindow<ErrorT>> failedInserts = Lists.newArrayList();
    BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
    for (Map.Entry<String, List<ValueInSingleWindow<TableRow>>> entry : tableRows.entrySet()) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(entry.getKey());
      flushRows(
          tableReference,
          entry.getValue(),
          uniqueIdsForTableRows.get(entry.getKey()),
          options,
          failedInserts);
    }
    tableRows.clear();
    uniqueIdsForTableRows.clear();

    for (ValueInSingleWindow<ErrorT> row : failedInserts) {
      context.output(failedOutputTag, row.getValue(), row.getTimestamp(), row.getWindow());
    }
  }

  /** Tracks operation counts, latencies, and bytes, for individual operations. Since its not always possible to compute
   * rates value for counters, we maintain some basic rates in OpTracker.
   */
  private class OpTracker {
    final class OpBucket {
      long opCount;
      long opCount1s;
      long opCount10s;
      long opCount30s;
      long bytes;
    };
    SortedMap<Long /* ts bucket */, OpBucket> opBuckets = new TreeMap<>();
    long latestTimestampMillis = 0L;

    public synchronized void addOp(long timestampMillis, long latencyMillis, long totalBytes) {
      // Use 5 second buckets to keep a 60-second sliding window.
      long bucketKey = (timestampMillis / 5) * 5;
      OpBucket bucket = opBuckets.get(bucketKey);
      bucket.opCount++;
      if (latencyMillis > 1e3) {
        bucket.opCount1s++;
      }
      if (latencyMillis > 1e4) {
        bucket.opCount10s++;
      }
      if (latencyMillis > 3e4) {
        bucket.opCount30s++;
      }
      bucket.bytes += totalBytes;
      latestTimestampMillis = Math.max(latestTimestampMillis, timestampMillis);
      updateCounters(totalBytes);
    }

    private void updateCounters(long totalBytes) {
      // Retain only the last 60 sec.
      opBuckets.entrySet().removeIf(e->e.getKey()<latestTimestampMillis-60e3);
      // Compute totals
      OpBucket totals = new OpBucket();
      opBuckets.forEach((k, v)-> {
        totals.opCount += v.opCount;
        totals.opCount1s += v.opCount1s;
        totals.opCount10s += v.opCount10s;
        totals.opCount30s += v.opCount30s;
        totals.bytes += v.bytes;
      });

      SinkMetrics.writeOpsRate60s().set(totals.opCount);
      SinkMetrics.writeOpsRate60sWithLatencyGt1s().set(totals.opCount1s);
      SinkMetrics.writeOpsRate60sWithLatencyGt10s().set(totals.opCount10s);
      SinkMetrics.writeOpsRate60sWithLatencyGt30s().set(totals.opCount30s);
      SinkMetrics.bytesWrittenRate60s().set(totals.bytes);

      SinkMetrics.bytesWritten().inc(totalBytes);
    }
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  private void flushRows(
      TableReference tableReference,
      List<ValueInSingleWindow<TableRow>> tableRows,
      List<String> uniqueIds,
      BigQueryOptions options,
      List<ValueInSingleWindow<ErrorT>> failedInserts)
      throws InterruptedException {
    if (!tableRows.isEmpty()) {
      try {
        long startTime = System.currentTimeMillis();
        long totalBytes =
            bqServices
                .getDatasetService(options)
                .insertAll(
                    tableReference,
                    tableRows,
                    uniqueIds,
                    retryPolicy,
                    failedInserts,
                    errorContainer,
                    skipInvalidRows,
                    ignoreUnknownValues);
        long finishTime = System.currentTimeMillis();
        tracker.addOp(finishTime - startTime, finishTime, totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
