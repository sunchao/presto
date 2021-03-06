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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RecordServiceTableLayoutHandle implements ConnectorTableLayoutHandle
{
  private final RecordServiceTableHandle table;
  private final List<RecordServiceColumnHandle> columns;

  @JsonCreator
  public RecordServiceTableLayoutHandle(@JsonProperty("table") RecordServiceTableHandle table,
      @JsonProperty("columns") List<RecordServiceColumnHandle> columns)
  {
    this.table = requireNonNull(table, "table is null");
    this.columns = requireNonNull(columns, "columns is null");
  }

  @JsonProperty
  public RecordServiceTableHandle getTable()
  {
    return table;
  }

  @JsonProperty
  public List<RecordServiceColumnHandle> getColumns()
  {
    return columns;
  }

  @JsonProperty
  public String getQuery()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    for (RecordServiceColumnHandle col : columns) {
      sb.append(col.getName());
      sb.append(",");
    }
    if (columns.size() == 0) {
      sb.append("*");
    } else {
      sb.setLength(sb.length() - 1);
    }
    sb.append(" FROM ");
    sb.append(table.getSchemaTableName());
    return sb.toString();
  }

  @Override
  public String toString()
  {
    return table.toString();
  }
}
