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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class RecordServiceColumnHandle implements ColumnHandle
{
  private final String connectorId;
  private final String name;
  private final Type type;

  @JsonCreator
  public RecordServiceColumnHandle(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("name") String name,
      @JsonProperty("type") Type type)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.name = requireNonNull(name, "name is null");
    this.type = requireNonNull(type, "type is null");
  }

  @JsonProperty
  public String getConnectorId()
  {
    return connectorId;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Type getType()
  {
    return type;
  }

  // TODO: add hashcode and equals
}
