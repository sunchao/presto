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
    // TODO: get request info from layout
    Request request = Request.createSqlRequest("select * from tpch.nation");
    try {
      PlanRequestResult planRequestResult = RecordServiceClient.getPlanResult(config, request);
      List<ConnectorSplit> splits = new ArrayList<>();
      for (Task task : planRequestResult.tasks) {
        // TODO: is schema info required here?
        splits.add(new RecordServiceSplit(connectorId, task, planRequestResult.hosts));
      }
      Collections.shuffle(splits);

      return new FixedSplitSource(connectorId, splits);
    }
    catch (IOException e) {
      log.error("Failed to getSplits.", e);
    }
    catch (RecordServiceException e) {
      log.error("Failed to getSplits", e);
    }

    return null;
  }
}
