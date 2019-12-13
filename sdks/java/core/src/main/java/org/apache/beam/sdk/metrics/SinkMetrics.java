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
package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/** Standard Sink Metrics. */
@Experimental(Kind.METRICS)
public class SinkMetrics {

  private static final String SINK_NAMESPACE = "sink";

  private static final String ELEMENTS_WRITTEN = "elements_written";
  private static final String BYTES_WRITTEN = "bytes_written";
  private static final String BYTES_WRITTEN_RATE_60S = "bytes_written_rate_60s";
  private static final String WRITE_OPS_RATE_60S = "write_ops_rate_60s";
  private static final String WRITE_OPS_RATE_60S_WITH_LATENCY_GT_1S =
          "write_ops_rate_60s_with_latency_gt_1s";
  private static final String WRITE_OPS_RATE_60S_WITH_LATENCY_GT_10S =
          "write_ops_rate_60s_with_latency_gt_10s";
  private static final String WRITE_OPS_RATE_60S_WITH_LATENCY_GT_30S =
          "write_ops_rate_60s_with_latency_gt_30s";

  private static final Counter ELEMENTS_WRITTEN_COUNTER =
          Metrics.counter(SINK_NAMESPACE, ELEMENTS_WRITTEN);
  private static final Counter BYTES_WRITTEN_COUNTER =
          Metrics.counter(SINK_NAMESPACE, BYTES_WRITTEN);
  private static final Gauge BYTES_WRITTEN_RATE_60S_COUNTER =
          Metrics.gauge(SINK_NAMESPACE, BYTES_WRITTEN_RATE_60S);
  private static final Gauge WRITE_OPS_RATE_60S_COUNTER =
          Metrics.gauge(SINK_NAMESPACE, WRITE_OPS_RATE_60S);
  private static final Gauge WRITE_OPS_RATE_60S_WITH_LATENCY_GT_1S_COUNTER =
          Metrics.gauge(SINK_NAMESPACE, WRITE_OPS_RATE_60S_WITH_LATENCY_GT_1S);
  private static final Gauge WRITE_OPS_RATE_60S_WITH_LATENCY_GT_10S_COUNTER =
          Metrics.gauge(SINK_NAMESPACE, WRITE_OPS_RATE_60S_WITH_LATENCY_GT_10S);
  private static final Gauge WRITE_OPS_RATE_60S_WITH_LATENCY_GT_30S_COUNTER =
          Metrics.gauge(SINK_NAMESPACE, WRITE_OPS_RATE_60S_WITH_LATENCY_GT_30S);

  /** Counter of elements written to a sink. */
  public static Counter elementsWritten() {
    return ELEMENTS_WRITTEN_COUNTER;
  }

  /** Counter of bytes written to a sink. */
  public static Counter bytesWritten() {
    return BYTES_WRITTEN_COUNTER;
  }

  /** Counter of write operations per 60 second interval. */
  public static Gauge bytesWrittenRate60s() {
    return BYTES_WRITTEN_RATE_60S_COUNTER;
  }
  /** Counter of write operations per 60 second interval. */
  public static Gauge writeOpsRate60s() {
    return WRITE_OPS_RATE_60S_COUNTER;
  }

  /** Counter of write operations per 60 second interval. */
  public static Gauge writeOpsRate60sWithLatencyGt1s() {
    return WRITE_OPS_RATE_60S_WITH_LATENCY_GT_1S_COUNTER;
  }

  /** Counter of write operations per 60 second interval. */
  public static Gauge writeOpsRate60sWithLatencyGt10s() {
    return WRITE_OPS_RATE_60S_WITH_LATENCY_GT_10S_COUNTER;
  }

  /** Counter of write operations per 60 second interval. */
  public static Gauge writeOpsRate60sWithLatencyGt30s() {
    return WRITE_OPS_RATE_60S_WITH_LATENCY_GT_30S_COUNTER;
  }
}
