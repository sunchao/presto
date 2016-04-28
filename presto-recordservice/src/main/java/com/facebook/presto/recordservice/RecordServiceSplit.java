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
import com.cloudera.recordservice.core.Schema;
import com.cloudera.recordservice.core.Task;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.List;

public class RecordServiceSplit implements ConnectorSplit
{
  private final String connectorId;
  private final Task task;
  private final Schema schema;

  public RecordServiceSplit(String connectorId, Task task, Schema schema)
  {
    this.connectorId = connectorId;
    this.task = task;
    this.schema = schema;
  }

  @Override
  public boolean isRemotelyAccessible()
  {
    return true;
  }

  @Override
  public List<HostAddress> getAddresses()
  {
    List<HostAddress> list = new LinkedList<>();
    for (NetworkAddress add : task.localHosts) {
      list.add(HostAddress.fromParts(add.hostname, add.port));
    }
    return list;
  }

  @Override
  public Object getInfo()
  {
    return this;
  }

  public Task getTask()
  {
    return task;
  }

  public Schema getSchema()
  {
    return schema;
  }

  @JsonProperty
  public String getConnectorId()
  {
    return connectorId;
  }
}
