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

import com.cloudera.recordservice.core.Schema;
import com.cloudera.recordservice.core.Schema.TypeDesc;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.recordservice.Types.checkType;
import static java.util.Objects.requireNonNull;

public class RecordServiceMetadata implements ConnectorMetadata
{
  private static final Logger LOG = Logger.get(RecordServiceMetadata.class);
  private final String connectorId;

  @Inject
  public RecordServiceMetadata(
      RecordServiceConnectorId connectorId)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session)
  {
    return RecordServiceClient.getDatabases();
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
  {
    requireNonNull(tableName, "tableName is null");
    // TODO: check table presence?
    return new RecordServiceTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
      ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
  {
    RecordServiceTableHandle handle = checkType(table, RecordServiceTableHandle.class, "table");
    return ImmutableList.of(new ConnectorTableLayoutResult(
        getTableLayout(session, new RecordServiceTableLayoutHandle(handle)), constraint.getSummary()));
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
  {
    RecordServiceTableLayoutHandle rsHandle = checkType(handle, RecordServiceTableLayoutHandle.class, "handle");
    return new ConnectorTableLayout(rsHandle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
  {
    return getTableMetadata(schemaTableName(table));
  }

  private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
  {
    Schema schema = RecordServiceClient.getSchema(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    return new ConnectorTableMetadata(schemaTableName, extractColumnMetadataList(schema));
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
  {
    return RecordServiceClient.getTables()
        .stream().map(tblName -> new SchemaTableName(schemaNameOrNull, tblName))
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
  {
    requireNonNull(tableHandle, "tableHandle is null");
    SchemaTableName schemaTableName = schemaTableName(tableHandle);
    Schema schema = RecordServiceClient.getSchema(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
    for (Schema.ColumnDesc columnDesc : schema.cols) {
      columnHandles.put(columnDesc.name, new RecordServiceColumnHandle(connectorId, columnDesc.name, convertType(columnDesc.type)));
    }
    return columnHandles.build();
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
  {
    RecordServiceColumnHandle rsColumnHandle = checkType(columnHandle, RecordServiceColumnHandle.class, "columnHandle");
    return new ColumnMetadata(rsColumnHandle.getName(), rsColumnHandle.getType());
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
  {
    requireNonNull(prefix, "prefix is null");
    ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
    for (SchemaTableName tableName : listTables(session, prefix)) {
      columns.put(tableName, getTableMetadata(tableName).getColumns());
    }
    return columns.build();
  }

  public static SchemaTableName schemaTableName(ConnectorTableHandle table)
  {
    requireNonNull(table, "table is null");
    return checkType(table, RecordServiceTableHandle.class, "table").getSchemaTableName();
  }

  private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
  {
    if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
      return listTables(session, prefix.getSchemaName());
    }
    return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
  }

  private List<ColumnMetadata> extractColumnMetadataList(Schema schema) {
    return schema.cols.stream()
        .map(columnDesc -> new ColumnMetadata(columnDesc.name, convertType(columnDesc.type)))
        .collect(Collectors.toList());
  }

  private Type convertType(TypeDesc typeDesc) {
    switch (typeDesc.typeId) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
      case INT:
        return IntegerType.INTEGER;
      case BIGINT:
        return BigintType.BIGINT;
      case FLOAT:
      case DOUBLE:
        return DoubleType.DOUBLE;
      case STRING:
        return VarbinaryType.VARBINARY;
      case VARCHAR:
        return VarcharType.VARCHAR;
      case CHAR:
        return VarcharType.createVarcharType(typeDesc.len);
      case TIMESTAMP_NANOS:
        return TimestampType.TIMESTAMP;
      case DECIMAL:
        return DecimalType.createDecimalType(typeDesc.precision, typeDesc.len);
      default:
        throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR,
            "Unsupported RecordService type " + typeDesc.typeId.name());
    }
  }
}
