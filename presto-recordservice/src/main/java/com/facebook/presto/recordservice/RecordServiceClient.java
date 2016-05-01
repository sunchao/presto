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

import com.cloudera.recordservice.core.DelegationToken;
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
import java.util.List;
import java.util.Random;

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
      return getPlannerBuilder().getDatabases(plannerAddr.getHostText(), plannerAddr.getPort());
    } catch (IOException | RecordServiceException e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, "Failed at getDatabases", e);
    }
  }

  public List<String> getTables(String db)
  {
    try {
      HostAddress plannerAddr = getPlannerHostAddress();
      return getPlannerBuilder()
          .getTables(plannerAddr.getHostText(), plannerAddr.getPort(), db);
    } catch (IOException | RecordServiceException e) {
      throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR, "Failed at getTables", e);
    }
  }

  public Schema getSchema(String db, String table) {
    try {
      HostAddress plannerAddr = getPlannerHostAddress();
      Request request = Request.createTableScanRequest(db + "." + table);
      return getPlannerBuilder()
          .getSchema(plannerAddr.getHostText(), plannerAddr.getPort(), request).schema;
    } catch (IOException | RecordServiceException e) {
      throw new PrestoException(RecordServiceErrorCode.PLAN_ERROR, "Failed at getSchema", e);
    }
  }

  public RecordServicePlanResult getPlanResult(Request request)
  {
    RecordServicePlannerClient plannerClient = null;
    try {
      HostAddress plannerAddr = getPlannerHostAddress();
      LOG.info("Get planResult from: " + plannerAddr);
      RecordServicePlannerClient.Builder builder = getPlannerBuilder();
      plannerClient = builder.connect(plannerAddr.getHostText(), plannerAddr.getPort());
      PlanRequestResult planRequestResult = plannerClient.planRequest(request);
      DelegationToken delegationToken = null;
      if (plannerClient.isKerberosAuthenticated()) {
         delegationToken = plannerClient.getDelegationToken("");
      }
      return new RecordServicePlanResult(planRequestResult, delegationToken);
    } catch (IOException | RecordServiceException e) {
      throw new PrestoException(RecordServiceErrorCode.PLAN_ERROR, "Failed at planRequest", e);
    } finally {
      if (plannerClient != null) {
        plannerClient.close();
      }
    }
  }

  public static HostAddress getWorkerHostAddress(List<HostAddress> addresses, List<HostAddress> globalAddresses)
  {
    String localHost;
    try {
      localHost = InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException e) {
      throw new PrestoException(RecordServiceErrorCode.TASK_ERROR, "Failed to get the local host.", e);
    }

    HostAddress address = null;

    // 1. If the data is available on this node, schedule the task locally.
    for (HostAddress add : addresses) {
      if (localHost.equals(add.getHostText())) {
        LOG.info("Both data and RecordServiceWorker are available locally for task.");
        address = add;
        break;
      }
    }

    // 2. Check if there's a RecordServiceWorker running locally. If so, pick that node.
    if (address == null) {
      for (HostAddress loc : globalAddresses) {
        if (localHost.equals(loc.getHostText())) {
          address = loc;
          LOG.info("RecordServiceWorker is available locally for task");
          break;
        }
      }
    }

    // 3. Finally, we don't have RecordServiceWorker running locally. Randomly pick
    // a node from the global membership.
    if (address == null) {
      Random rand = new Random();
      address = globalAddresses.get(rand.nextInt(globalAddresses.size()));
      LOG.info("Neither RecordServiceWorker nor data is available locally for task {}." +
          " Randomly selected host {} to execute it", address);
    }

    return address;
  }

  private HostAddress getPlannerHostAddress()
  {
    // size is checked in getPlanners() call
    return config.getPlanners().iterator().next();
  }

  private RecordServicePlannerClient.Builder getPlannerBuilder()
  {
    RecordServicePlannerClient.Builder result = new RecordServicePlannerClient.Builder();
    if (config.getKerberosPrincipal() != null) {
      result.setKerberosPrincipal(config.getKerberosPrincipal());
    }
    return result;
  }
}
