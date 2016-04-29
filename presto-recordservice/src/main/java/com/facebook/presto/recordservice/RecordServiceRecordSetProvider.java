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

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;

import java.util.List;

import javax.inject.Inject;

import static com.facebook.presto.recordservice.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RecordServiceRecordSetProvider implements ConnectorRecordSetProvider
{
  private final String connectorId;
  private static final Logger log = Logger.get(RecordServiceRecordSetProvider.class);

  @Inject
  public RecordServiceRecordSetProvider(RecordServiceConnectorId connectorId)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
  }

  @Override
  public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
  {
    requireNonNull(split, "split is null");
    RecordServiceSplit recordServiceSplit = checkType(split, RecordServiceSplit.class, "split");
    checkArgument(recordServiceSplit.getConnectorId().equals(connectorId),
        "split is not for this connector");

    return new RecordServiceRecordSet(recordServiceSplit);
  }
}
