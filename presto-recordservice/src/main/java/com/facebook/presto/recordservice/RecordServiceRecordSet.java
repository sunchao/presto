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
import com.cloudera.recordservice.core.Task;
import com.cloudera.recordservice.thrift.TNetworkAddress;
import com.cloudera.recordservice.thrift.TTask;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class RecordServiceRecordSet implements RecordSet, Closeable
{
  private static final Logger log = Logger.get(RecordServiceRecordSet.class);
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
      records = workerClient.execAndFetch(fromSplit(split));
      records.setCloseWorker(true);
    }
    catch (RecordServiceException e) {
      log.error("Failed to fetch records.", e);
    }
    catch (IOException e) {
      log.error("Failed to fetch records.", e);
    }
    finally {
      if (records == null && workerClient != null) {
        workerClient.close();
      }
    }
    this.records = records;
  }

  @Override
  public List<Type> getColumnTypes()
  {
    return records.getSchema().cols.stream()
        .map(columnDesc -> RecordServiceUtil.convertType(columnDesc.type))
        .collect(Collectors.toList());
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

  private Task fromSplit(RecordServiceSplit split) {
    TTask thriftTask = new TTask();
    thriftTask.setTask(split.getTask());
    thriftTask.setLocal_hosts(
        split.getAddresses().stream()
            .map(addr -> new TNetworkAddress(addr.getHostText(), addr.getPort()))
            .collect(Collectors.toList()));
    thriftTask.setResults_ordered(split.getResultOrdered());
    thriftTask.setTask_size(split.getTaskSize());
    thriftTask.setTask_id(new TUniqueId(split.getHi(), split.getLo()));
    return new Task(thriftTask);
  }
}
