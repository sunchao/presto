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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;

import java.util.Map;

import static java.util.Objects.requireNonNull;

import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;

public class RecordServiceConnectorFactory implements ConnectorFactory {
  private static final Logger log = Logger.get(RecordServiceConnectorFactory.class);
  private final TypeManager typeManager;
  private final NodeManager nodeManager;
  private final Map<String, String> optionalConfig;

  RecordServiceConnectorFactory(
      TypeManager typeManager,
      NodeManager nodeManager,
      Map<String, String> optionalConfig)
  {
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
    this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    this.optionalConfig = requireNonNull(optionalConfig, "optionalConfig is null");
  }

  @Override
  public String getName() {
    return "recordservice";
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new RecordServiceHandleResolver();
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config) {
    requireNonNull(connectorId, "connectorId is null");
    requireNonNull(config, "config is null");
    log.info("Start to create a RecordServiceConnector");

    try {
      Bootstrap app = new Bootstrap(
          new JsonModule(),
          new RecordServiceModule(connectorId, typeManager)
      );

      Injector injector = app.strictConfig()
          .doNotInitializeLogging()
          .setRequiredConfigurationProperties(config)
          .setOptionalConfigurationProperties(optionalConfig)
          .initialize();

      return injector.getInstance(RecordServiceConnector.class);
    }
    catch(Exception e) {
      log.error("Failed to create a RecordServiceConnector.", e);
      throw Throwables.propagate(e);
    }
  }
}
