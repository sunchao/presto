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
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.io.IOException;

public class RecordServiceRecordCursor implements RecordCursor
{
  private static final Logger log = Logger.get(RecordServiceSplitManager.class);
  private final Records records;
  private Records.Record nextRecord;

  public RecordServiceRecordCursor(Records records)
  {
    this.records = records;
    this.nextRecord = null;
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
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean advanceNextPosition()
  {
    try {
      return nextRecord != null || records.hasNext();
    }
    catch (IOException e) {
      log.error("Failed to advanceNextPosition.", e);
    }
    catch (RecordServiceException e) {
      log.error("Failed to advanceNextPosition.", e);
    }
    return false;
  }

  @Override
  public boolean getBoolean(int field)
  {
    try {
      if (nextRecord == null) {
        nextRecord = records.next();
      }
      Records.Record curRecord = nextRecord;
      nextRecord = records.next();
      return curRecord.nextBoolean(field);
    }
    catch (IOException e) {
      log.error("Failed to getBoolean.", e);
    }
    throw new RuntimeException("Failed to getBoolean.");
  }

  @Override
  public long getLong(int field)
  {
    try {
      if (nextRecord == null) {
        nextRecord = records.next();
      }
      Records.Record curRecord = nextRecord;
      nextRecord = records.next();
      return curRecord.nextLong(field);
    }
    catch (IOException e) {
      log.error("Failed to getLong.", e);
    }
    throw new RuntimeException("Failed to getLong.");
  }

  @Override
  public double getDouble(int field)
  {
    try {
      if (nextRecord == null) {
        nextRecord = records.next();
      }
      Records.Record curRecord = nextRecord;
      nextRecord = records.next();
      return curRecord.nextDouble(field);
    }
    catch (IOException e) {
      log.error("Failed to getDouble.", e);
    }
    throw new RuntimeException("Failed to getDouble.");
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
    try {
      if (nextRecord == null) nextRecord = records.next();
      return nextRecord == null || nextRecord.isNull(field);
    } catch (IOException e) {
      log.error("Failed to isNull.", e);
    }
    throw new RuntimeException("Failed to isNull.");
  }

  @Override
  public void close()
  {
    if (records != null) {
      records.close();
    }
  }
}
