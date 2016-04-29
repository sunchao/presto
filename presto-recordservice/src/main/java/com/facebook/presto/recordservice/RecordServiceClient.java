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

public class RecordServiceClient {

  private static final Logger log = Logger.get(RecordServiceClient.class);

  public static List<String> getDatabases()
  {
    try {
      return new RecordServicePlannerClient.Builder().getDatabases("172.21.2.110", 12050);
    } catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, e);
    }
  }

  public static List<String> getTables()
  {
    try {
      return new RecordServicePlannerClient.Builder().getTables("172.21.2.110", 12050, "tpch");
    } catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, e);
    }
  }

  public static Schema getSchema(String db, String table) {
    try {
      Request request = Request.createTableScanRequest(db + "." + table);
      return new RecordServicePlannerClient.Builder().getSchema("172.21.2.110", 12050, request).schema;
    } catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, e);
    }
  }

  public static PlanRequestResult getPlanResult(RecordServiceConnectorConfig config,
      Request request) throws IOException, RecordServiceException
  {
    Set<HostAddress> planners = config.getPlanners();
    List<HostAddress> plannerList = new ArrayList<>(planners);
    Collections.shuffle(plannerList);
    String plannerHost = plannerList.get(0).getHostText();
    int port = plannerList.get(0).getPort();
    log.info("Get planResult from " + plannerList.get(0).toString());

    return new RecordServicePlannerClient.Builder().planRequest(plannerHost, port, request);
  }

  public static HostAddress getWorkerHostAddress(List<HostAddress> addresses)
  {
    String localHost = null;
    try {
      localHost = InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException e) {
      log.error("Failed to get the local host.", e);
    }

    // 1. If the data is available on this node, schedule the task locally.
    for (HostAddress add : addresses) {
      if (localHost.equals(add.getHostText())) {
        log.info("Both data and RecordServiceWorker are available locally for task.");
        return add;
      }
    }

    // 2. Otherwise, randomly pick a node.
    Collections.shuffle(addresses);

    return addresses.get(0);
  }
}
