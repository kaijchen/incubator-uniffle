/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.coordinator.metric;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.common.metrics.MetricsManager;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;

public class CoordinatorMetrics {

  private static final String TOTAL_SERVER_NUM = "total_server_num";
  private static final String RUNNING_APP_NUM = "running_app_num";
  private static final String TOTAL_APP_NUM = "total_app_num";
  private static final String EXCLUDE_SERVER_NUM = "exclude_server_num";
  private static final String UNHEALTHY_SERVER_NUM = "unhealthy_server_num";
  private static final String TOTAL_ACCESS_REQUEST = "total_access_request";
  private static final String TOTAL_CANDIDATES_DENIED_REQUEST = "total_candidates_denied_request";
  private static final String TOTAL_LOAD_DENIED_REQUEST = "total_load_denied_request";
  private static final String TOTAL_QUOTA_DENIED_REQUEST = "total_quota_denied_request";
  public static final String REMOTE_STORAGE_IN_USED_PREFIX = "remote_storage_in_used_";
  public static final String APP_NUM_TO_USER = "app_num";
  public static final String USER_LABEL = "user_name";

  private final Gauge gaugeTotalServerNum;
  private final Gauge gaugeExcludeServerNum;
  private final Gauge gaugeUnhealthyServerNum;
  private final Gauge gaugeRunningAppNum;
  private final Gauge gaugeRunningAppNumToUser;
  private final Counter counterTotalAppNum;
  private final Counter counterTotalAccessRequest;
  private final Counter counterTotalCandidatesDeniedRequest;
  private final Counter counterTotalQuotaDeniedRequest;
  private final Counter counterTotalLoadDeniedRequest;
  private final Map<String, Gauge> gaugeUsedRemoteStorage = JavaUtils.newConcurrentMap();

  private final MetricsManager metricsManager;

  private static CoordinatorMetrics instance;

  private CoordinatorMetrics(CollectorRegistry collectorRegistry) {
    metricsManager = new MetricsManager(collectorRegistry);
    gaugeTotalServerNum = metricsManager.addGauge(TOTAL_SERVER_NUM);
    gaugeExcludeServerNum = metricsManager.addGauge(EXCLUDE_SERVER_NUM);
    gaugeUnhealthyServerNum = metricsManager.addGauge(UNHEALTHY_SERVER_NUM);
    gaugeRunningAppNum = metricsManager.addGauge(RUNNING_APP_NUM);
    gaugeRunningAppNumToUser = metricsManager.addGauge(APP_NUM_TO_USER, USER_LABEL);
    counterTotalAppNum = metricsManager.addCounter(TOTAL_APP_NUM);
    counterTotalAccessRequest = metricsManager.addCounter(TOTAL_ACCESS_REQUEST);
    counterTotalCandidatesDeniedRequest = metricsManager.addCounter(TOTAL_CANDIDATES_DENIED_REQUEST);
    counterTotalQuotaDeniedRequest = metricsManager.addCounter(TOTAL_QUOTA_DENIED_REQUEST);
    counterTotalLoadDeniedRequest = metricsManager.addCounter(TOTAL_LOAD_DENIED_REQUEST);
  }

  public static synchronized void register(CollectorRegistry collectorRegistry) {
    if (instance == null) {
      instance = new CoordinatorMetrics(collectorRegistry);
    }
  }

  @VisibleForTesting
  public static void register() {
    register(CollectorRegistry.defaultRegistry);
  }

  @VisibleForTesting
  public static synchronized void clear() {
    instance = null;
    CollectorRegistry.defaultRegistry.clear();
  }

  public static Gauge getTotalServerNumGauge() {
    return instance.gaugeTotalServerNum;
  }

  public static Gauge getExcludeServerNumGauge() {
    return instance.gaugeExcludeServerNum;
  }

  public static Gauge getUnhealthyServerNumGauge() {
    return instance.gaugeUnhealthyServerNum;
  }

  public static Gauge getRunningAppNumGauge() {
    return instance.gaugeRunningAppNum;
  }

  public static Gauge getRunningAppNumToUserGauge() {
    return instance.gaugeRunningAppNumToUser;
  }

  public static Counter getTotalAppNumCounter() {
    return instance.counterTotalAppNum;
  }

  public static Counter getTotalAccessRequestCounter() {
    return instance.counterTotalAccessRequest;
  }

  public static Counter getTotalCandidatesDeniedRequestCounter() {
    return instance.counterTotalCandidatesDeniedRequest;
  }

  public static Counter getTotalQuotaDeniedRequestCounter() {
    return instance.counterTotalQuotaDeniedRequest;
  }

  public static Counter getTotalLoadDeniedRequestCounter() {
    return instance.counterTotalLoadDeniedRequest;
  }

  public static CollectorRegistry getCollectorRegistry() {
    return instance.metricsManager.getCollectorRegistry();
  }

  public static void addDynamicGaugeForRemoteStorage(String storageHost) {
    if (!StringUtils.isEmpty(storageHost)) {
      if (!instance.gaugeUsedRemoteStorage.containsKey(storageHost)) {
        String metricName = REMOTE_STORAGE_IN_USED_PREFIX + RssUtils.getMetricNameForHostName(storageHost);
        instance.gaugeUsedRemoteStorage.putIfAbsent(storageHost,
            instance.metricsManager.addGauge(metricName));
      }
    }
  }

  public static void updateDynamicGaugeForRemoteStorage(String storageHost, double value) {
    if (instance.gaugeUsedRemoteStorage.containsKey(storageHost)) {
      instance.gaugeUsedRemoteStorage.get(storageHost).set(value);
    }
  }

  public static Gauge getDynamicGaugeForRemoteStorage(String storageHost) {
    return instance.gaugeUsedRemoteStorage.get(storageHost);
  }
}
