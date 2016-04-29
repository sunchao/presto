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
import com.cloudera.recordservice.core.RecordServiceWorkerClient;
import com.cloudera.recordservice.core.Records;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.io.ByteSource;
import io.airlift.log.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class RecordServiceRecordSet implements RecordSet, Closeable
{
  private static final Logger log = Logger.get(RecordServiceRecordSet.class);
//  private final List<Type> columnTypes;
//  private final ByteSource byteSource;
  private RecordServiceWorkerClient workerClient = null;
  private Records records = null;

  public RecordServiceRecordSet(RecordServiceSplit split)
  {
    HostAddress host = RecordServiceClient.getWorkerHostAddress(split.getAddresses());

    RecordServiceWorkerClient workerClient = null;
    Records records = null;
    try {
      workerClient =
          new RecordServiceWorkerClient.Builder().connect(host.getHostText(), host.getPort());
      records = workerClient.execAndFetch(split.getTask());
      records.setCloseWorker(true);
    } catch (RecordServiceException e) {
      log.error("Failed to fetch records.", e);
    } catch (IOException e) {
      log.error("Failed to fetch records.", e);
    } finally {
      if (records == null && workerClient != null) {
        workerClient.close();
      }
    }
    this.records = records;
  }

  @Override
  public List<Type> getColumnTypes()
  {
    return null;
  }

  @Override
  public RecordCursor cursor()
  {
    return new RecordServiceRecordCursor(records);
  }

  @Override
  public void close() throws IOException
  {
    if (records != null) records.close();
    if (workerClient != null) workerClient.close();
  }
}
