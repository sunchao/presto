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

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Schema;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class RecordServiceRecordCursor implements RecordCursor
{
  private static final Logger log = Logger.get(RecordServiceSplitManager.class);
  private final Records records;
  private Records.Record nextRecord;
  private List<Schema.TypeDesc> columnTypes;
  private boolean[] booleanVals;
  private long[] longVals;
  private double[] doubleVals;
  private Slice[] sliceVals;

  public RecordServiceRecordCursor(Records records)
  {
    this.records = records;
    this.nextRecord = null;
    this.columnTypes = records.getSchema().cols.stream()
        .map(columnDesc -> columnDesc.type).collect(Collectors.toList());
    booleanVals = new boolean[columnTypes.size()];
    longVals = new long[columnTypes.size()];
    doubleVals = new double[columnTypes.size()];
    sliceVals = new Slice[columnTypes.size()];
  }

  @Override
  public long getTotalBytes()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getCompletedBytes()
  {
    try {
      return records.getStatus().stats.bytesRead;
    }
    catch (IOException e) {
      log.error("Failed to getCompletedBytes.", e);
    }
    catch (RecordServiceException e) {
      log.error("Failed to getCompletedBytes.", e);
    }
    throw new RuntimeException("Failed to getCompletedBytes");
  }

  @Override
  public long getReadTimeNanos()
  {
    try {
      return records.getStatus().stats.clientTimeMs;
    }
    catch (IOException e) {
      log.error("Failed to getReadTimeNanos.", e);
    }
    catch (RecordServiceException e) {
      log.error("Failed to getReadTimeNanos.", e);
    }
    throw new RuntimeException("Failed to getReadTimeNanos");
  }

  @Override
  public Type getType(int field)
  {
    return RecordServiceUtil.convertType(records.getSchema().cols.get(field).type);
  }

  @Override
  public boolean advanceNextPosition()
  {
    try {
      boolean result = records.hasNext();
      if (result) {
        nextRecord = records.next();
        for (int i = 0; i < columnTypes.size(); ++i)
        {
          switch (columnTypes.get(i).typeId)
          {
            case BOOLEAN:
              booleanVals[i] = nextRecord.nextBoolean(i);
              break;
            case TINYINT:
              longVals[i] = nextRecord.nextByte(i);
              break;
            case SMALLINT:
              longVals[i] = nextRecord.nextShort(i);
              break;
            case INT:
              longVals[i] = nextRecord.nextInt(i);
              break;
            case BIGINT:
              longVals[i] = nextRecord.nextLong(i);
              break;
            case FLOAT:
              doubleVals[i] = nextRecord.nextFloat(i);
              break;
            case DOUBLE:
              doubleVals[i] = nextRecord.nextDouble(i);
              break;
            case STRING:
            case VARCHAR:
            case CHAR:
              // TODO: avoid creating string?
              sliceVals[i] = Slices.utf8Slice(nextRecord.nextByteArray(i).toString());
              break;
            default:
              // TODO: handle decimal and timestamp
              throw new PrestoException(RecordServiceErrorCode.TYPE_ERROR,
                  "Unsupported type " + columnTypes.get(i).typeId);
          }
        }
      }
      return result;
    }
    catch (Exception e) {
      throw new PrestoException(RecordServiceErrorCode.CURSOR_ERROR, e);
    }
  }

  @Override
  public boolean getBoolean(int field)
  {
    return booleanVals[field];
  }

  @Override
  public long getLong(int field)
  {
    return longVals[field];
  }

  @Override
  public double getDouble(int field)
  {
    return doubleVals[field];
  }

  @Override
  public Slice getSlice(int field)
  {
    return sliceVals[field];
  }

  @Override
  public Object getObject(int field)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNull(int field)
  {
    return nextRecord.isNull(field);
  }

  @Override
  public void close()
  {
    if (records != null) {
      records.close();
    }
  }
}
