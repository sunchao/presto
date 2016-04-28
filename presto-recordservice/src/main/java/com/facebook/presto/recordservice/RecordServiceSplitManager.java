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
import com.cloudera.recordservice.core.Task;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RecordServiceSplitManager implements ConnectorSplitManager
{
  private final String connectorId;
  private static final Logger log = Logger.get(RecordServiceSplitManager.class);
  private final RecordServiceConnectorConfig config;

  public RecordServiceSplitManager(RecordServiceConnectorId  connectorId, RecordServiceConnectorConfig config)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.config = requireNonNull(config, "RecordServiceConfig is null");
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle,
      ConnectorSession session, ConnectorTableLayoutHandle layout)
  {
    Request request = null;
    Set<HostAddress> planners = config.getPlanners();
    List<HostAddress> plannerList = new ArrayList<>(planners);
    Collections.shuffle(plannerList);
    String plannerHost = plannerList.get(0).getHostText();
    int port = plannerList.get(0).getPort();
    try {
      PlanRequestResult planRequestResult = new RecordServicePlannerClient.Builder()
          .planRequest(plannerHost, port, request);
      List<ConnectorSplit> splits = new ArrayList<>();
      for (Task task : planRequestResult.tasks) {
        // TODO: Implement a RecordServiceSplitSource with schema info, instead of
        // wrapping the schema in each split.
        splits.add(new RecordServiceSplit(connectorId, task, planRequestResult.schema));
      }
      Collections.shuffle(splits);

      return new FixedSplitSource(connectorId, splits);
    }
    catch (IOException e) {
      log.error("Failed to getSplits.", e);
      return null;
    }
    catch (RecordServiceException e) {
      log.error("Failed to getSplits", e);
      return null;
    }
  }
}
