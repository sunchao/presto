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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RecordServiceSplit implements ConnectorSplit
{
  private final String connectorId;
  private final byte[] task;
  private final long taskSize;
  private final long hi;
  private final long lo;
  private final boolean resultOrdered;
  private final List<HostAddress> addresses;
  private final List<HostAddress> globalAddresses;

  // Parts for DelegationToken. If no delegation token, these are all null.
  private final String identifier;
  private final String password;
  private final byte[] token;

  @JsonCreator
  public RecordServiceSplit(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("task") byte[] task,
      @JsonProperty("taskSize") long taskSize,
      @JsonProperty("hi") long hi,
      @JsonProperty("lo") long lo,
      @JsonProperty("resultOrdered") boolean resultOrdered,
      @JsonProperty("addresses") List<HostAddress> addresses,
      @JsonProperty("globalAddresses") List<HostAddress> globalAddresses,
      @JsonProperty("identifier") String identifier,
      @JsonProperty("password") String password,
      @JsonProperty("token") byte[] token)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.task = requireNonNull(task, "task is null");
    this.taskSize = requireNonNull(taskSize, "taskSize is null");
    this.hi = hi;
    this.lo = lo;
    this.resultOrdered = resultOrdered;
    this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    this.globalAddresses = ImmutableList.copyOf(requireNonNull(globalAddresses, "globalAddresses is null"));

    // These are nullable
    this.identifier = identifier;
    this.password = password;
    this.token = token;
  }

  @Override
  public boolean isRemotelyAccessible()
  {
    return true;
  }

  @JsonProperty
  @Override
  public List<HostAddress> getAddresses()
  {
    return addresses;
  }

  @Override
  public Object getInfo()
  {
    return ImmutableMap.builder()
        .put("task", task)
        .put("taskSize", taskSize)
        .put("hi", hi)
        .put("lo", lo)
        .put("resultOrdered", resultOrdered)
        .put("addresses", addresses)
        .build();
  }

  @Override
  public String toString()
  {
    return toStringHelper(this)
        .addValue(task)
        .addValue(taskSize)
        .addValue(hi)
        .addValue(lo)
        .toString();
  }

  @JsonProperty
  public String getConnectorId()
  {
    return connectorId;
  }

  @JsonProperty
  public byte[] getTask()
  {
    return task;
  }

  @JsonProperty
  public long getTaskSize()
  {
    return taskSize;
  }

  @JsonProperty
  public long getHi()
  {
    return hi;
  }

  @JsonProperty
  public long getLo()
  {
    return lo;
  }

  @JsonProperty
  public boolean getResultOrdered()
  {
    return resultOrdered;
  }

  @JsonProperty
  public List<HostAddress> getGlobalAddresses()
  {
    return globalAddresses;
  }

  @JsonProperty
  public String getIdentifier()
  {
    return identifier;
  }

  @JsonProperty
  public String getPassword()
  {
    return password;
  }

  @JsonProperty
  public byte[] getToken()
  {
    return token;
  }
}
