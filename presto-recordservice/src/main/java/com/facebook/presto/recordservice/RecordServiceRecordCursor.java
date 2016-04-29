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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.io.IOException;

public class RecordServiceRecordCursor implements RecordCursor
{
  private Records records;

  public RecordServiceRecordCursor(Records records)
  {
    this.records = records;
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
    } catch (IOException e) {
      e.printStackTrace();
    } catch (RecordServiceException e) {
      e.printStackTrace();
    }
    throw new RuntimeException("Failed to getCompletedBytes");
  }

  @Override
  public long getReadTimeNanos()
  {
    try {
      return records.getStatus().stats.clientTimeMs;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (RecordServiceException e) {
      e.printStackTrace();
    }
    throw new RuntimeException("Failed to getReadTimeNanos");
  }

  @Override
  public Type getType(int field)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean advanceNextPosition()
  {
    try {
      return records.hasNext();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (RecordServiceException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean getBoolean(int field)
  {
    try {
      return records.next().nextBoolean(field);
    } catch (IOException e) {
      e.printStackTrace();
    }
    throw new RuntimeException("Failed to getCompletedBytes");
  }

  @Override
  public long getLong(int field)
  {
    try {
      return records.next().nextLong(field);
    } catch (IOException e) {
      e.printStackTrace();
    }
    throw new RuntimeException("Failed to getLong");
  }

  @Override
  public double getDouble(int field)
  {
    try {
      return records.next().nextDouble(field);
    } catch (IOException e) {
      e.printStackTrace();
    }
    throw new RuntimeException("Failed to getDouble");
  }

  @Override
  public Slice getSlice(int field)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getObject(int field)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNull(int field)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {
    if (records != null) {
      records.close();
    }
  }
}
