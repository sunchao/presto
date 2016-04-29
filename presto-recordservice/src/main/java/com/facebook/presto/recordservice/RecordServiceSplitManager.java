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

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.Request;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import io.airlift.log.Logger;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import static com.facebook.presto.recordservice.Types.checkType;
import static java.util.Objects.requireNonNull;

public class RecordServiceSplitManager implements ConnectorSplitManager
{
  private final String connectorId;
  private static final Logger log = Logger.get(RecordServiceSplitManager.class);
  private final RecordServiceClient client;

  @Inject
  public RecordServiceSplitManager(RecordServiceConnectorId  connectorId, RecordServiceConnectorConfig config)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    requireNonNull(config, "RecordServiceConfig is null");
    this.client = new RecordServiceClient(config);
  }

  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle,
      ConnectorSession session, ConnectorTableLayoutHandle layout)
  {
    RecordServiceTableLayoutHandle layoutHandle = checkType(layout,
        RecordServiceTableLayoutHandle.class, "layout");
    log.info("getSplits for " + layoutHandle.getQuery());

    Request request = Request.createSqlRequest(layoutHandle.getQuery());

    try {
      PlanRequestResult planRequestResult = client.getPlanResult(request);
      List<ConnectorSplit> splits = planRequestResult.tasks.stream()
          .map(t -> new RecordServiceSplit(
              connectorId, t.task, t.taskSize, t.taskId.hi,
              t.taskId.lo, t.resultsOrdered, toHostAddress(planRequestResult.hosts)))
          .collect(Collectors.toList());
      Collections.shuffle(splits);

      return new FixedSplitSource(connectorId, splits);
    }
    catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.PLAN_ERROR, e);
    }
  }

  private List<HostAddress> toHostAddress(List<NetworkAddress> addresses)
  {
    return addresses.stream().map(addr -> HostAddress.fromParts(addr.hostname, addr.port))
      .collect(Collectors.toList());
  }

}
