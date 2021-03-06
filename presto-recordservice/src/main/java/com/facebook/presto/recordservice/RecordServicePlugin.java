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

import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Presto plugin to use RecordService as a data source.
 */
public class RecordServicePlugin implements Plugin
{
  private static final Logger LOG = Logger.get(RecordServicePlugin.class);
  private TypeManager typeManager;
  private NodeManager nodeManager;
  private Map<String, String> optionalConfig = ImmutableMap.of();

  @Override
  public synchronized void setOptionalConfig(Map<String, String> optionalConfig)
  {
    this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
  }

  @Inject
  public synchronized void setTypeManager(TypeManager typeManager)
  {
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
  }

  @Inject
  public synchronized void setNodeManager(NodeManager nodeManager)
  {
    this.nodeManager = requireNonNull(nodeManager, "node is null");
  }

  @Override
  public <T> List<T> getServices(Class<T> type)
  {
    if (type == ConnectorFactory.class) {
      LOG.info("RecordServicePlugin GetServices()");
      return ImmutableList.of(type.cast(new RecordServiceConnectorFactory(typeManager, nodeManager, optionalConfig)));
    }

    return ImmutableList.of();
  }
}
