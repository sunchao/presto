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

import java.util.List;
import java.util.stream.Collectors;

import com.cloudera.recordservice.core.Schema;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

public class RecordServiceUtil
{
  public static List<ColumnMetadata> extractColumnMetadataList(Schema schema)
  {
    return schema.cols.stream()
        .map(columnDesc -> new ColumnMetadata(columnDesc.name, convertType(columnDesc.type)))
        .collect(Collectors.toList());
  }

  public static Type convertType(Schema.TypeDesc typeDesc)
  {
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
      case VARCHAR:
        return VarcharType.VARCHAR;
      case CHAR:
        return VarcharType.createVarcharType(typeDesc.len);
      case TIMESTAMP_NANOS:
        return TimestampType.TIMESTAMP;
      case DECIMAL:
        return DecimalType.createDecimalType(typeDesc.precision, typeDesc.scale);
      default:
        throw new PrestoException(RecordServiceErrorCode.CATALOG_ERROR,
            "Unsupported RecordService type " + typeDesc.typeId.name());
    }
  }
}
