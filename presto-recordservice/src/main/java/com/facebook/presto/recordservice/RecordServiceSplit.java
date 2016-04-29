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
import sun.nio.ch.Net;

import java.util.LinkedList;
import java.util.List;

public class RecordServiceSplit implements ConnectorSplit
{
  private final String connectorId;
  private final Task task;
  private final List<HostAddress> addresses;

  public RecordServiceSplit(String connectorId, Task task, List<NetworkAddress> hosts)
  {
    this.connectorId = connectorId;
    this.task = task;
    List<HostAddress> list = new LinkedList<>();
    for (NetworkAddress add : hosts) {
      list.add(HostAddress.fromParts(add.hostname, add.port));
    }
    this.addresses = list;
  }

  @Override
  public boolean isRemotelyAccessible()
  {
    return true;
  }

  @Override
  public List<HostAddress> getAddresses()
  {
    return addresses;
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

  @JsonProperty
  public String getConnectorId()
  {
    return connectorId;
  }
}
