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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RecordServiceTableHandle implements ConnectorTableHandle
{
  private final String connectorId;
  private final String schemaName;
  private final String tableName;

  @JsonCreator
  public RecordServiceTableHandle(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("schemaName") String schemaName,
      @JsonProperty("tableName") String tableName)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.schemaName = requireNonNull(schemaName, "schemaName is null");
    this.tableName = requireNonNull(tableName, "tableName is null");
  }

  @JsonProperty
  public String getConnectorId()
  {
    return connectorId;
  }

  @JsonProperty
  public String getSchemaName()
  {
    return schemaName;
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }

  public SchemaTableName getSchemaTableName()
  {
    return new SchemaTableName(schemaName, tableName);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordServiceTableHandle that = (RecordServiceTableHandle) o;
    if (connectorId != null ? !connectorId.equals(that.connectorId) : that.connectorId != null) {
      return false;
    }
    if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) {
      return false;
    }
    return tableName != null ? tableName.equals(that.tableName) : that.tableName == null;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(connectorId, schemaName, tableName);
  }
}
