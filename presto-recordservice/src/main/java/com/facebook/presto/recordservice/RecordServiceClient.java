/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.recordservice;

import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.Schema;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;

import io.airlift.log.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class RecordServiceClient {
  private static final Logger LOG = Logger.get(RecordServiceClient.class);
  private final RecordServiceConnectorConfig config;

  @Inject
  public RecordServiceClient(RecordServiceConnectorConfig config)
  {
    this.config = requireNonNull(config, "config is null");
  }

  public List<String> getDatabases()
  {
    try {
      HostAddress plannerAddr = getPlannerHostAddress();
      return new RecordServicePlannerClient.Builder()
          .getDatabases(plannerAddr.getHostText(), plannerAddr.getPort());
    } catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, e);
    }
  }

  public List<String> getTables(String db)
  {
    try {
      HostAddress plannerAddr = getPlannerHostAddress();
      return new RecordServicePlannerClient.Builder()
          .getTables(plannerAddr.getHostText(), plannerAddr.getPort(), db);
    } catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, e);
    }
  }

  public Schema getSchema(String db, String table) {
    try {
      HostAddress plannerAddr = getPlannerHostAddress();
      Request request = Request.createTableScanRequest(db + "." + table);
      return new RecordServicePlannerClient.Builder()
          .getSchema(plannerAddr.getHostText(), plannerAddr.getPort(), request).schema;
    } catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, e);
    }
  }

  public PlanRequestResult getPlanResult(Request request) throws IOException, RecordServiceException
  {
    HostAddress plannerAddr = getPlannerHostAddress();
    LOG.info("Get planResult from " + plannerAddr);
    return new RecordServicePlannerClient.Builder().planRequest(plannerAddr.getHostText(), plannerAddr.getPort(), request);
  }

  private HostAddress getPlannerHostAddress()
  {
    return config.getPlanners().iterator().next();
  }

  public static HostAddress getWorkerHostAddress(List<HostAddress> addresses)
  {
    String localHost = null;
    try {
      localHost = InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException e) {
      LOG.error("Failed to get the local host.", e);
    }

    // 1. If the data is available on this node, schedule the task locally.
    for (HostAddress add : addresses) {
      if (localHost.equals(add.getHostText())) {
        LOG.info("Both data and RecordServiceWorker are available locally for task.");
        return add;
      }
    }

    // 2. Otherwise, randomly pick a node.
    Collections.shuffle(addresses);

    return addresses.get(0);
  }
}
