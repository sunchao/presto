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

import javax.inject.Inject;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

public class RecordServiceModule implements Module
{
  private final String connectorId;
  private final TypeManager typeManager;

  public RecordServiceModule(String connectorId, TypeManager typeManager)
  {
    this.connectorId = requireNonNull(connectorId, "connectorId is null");
    this.typeManager = requireNonNull(typeManager, "typeManager is null");
  }

  @Override
  public void configure(Binder binder) {
    binder.bind(TypeManager.class).toInstance(typeManager);

    binder.bind(RecordServiceConnectorId.class).toInstance(new RecordServiceConnectorId(connectorId));
    binder.bind(RecordServiceConnector.class).in(Scopes.SINGLETON);
    binder.bind(RecordServiceMetadata.class).in(Scopes.SINGLETON);
    binder.bind(RecordServiceSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(RecordServiceRecordSetProvider.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(RecordServiceConnectorConfig.class);

    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
  }

  public static final class TypeDeserializer
      extends FromStringDeserializer<Type>
  {
    private static final long serialVersionUID = 1L;

    private final TypeManager typeManager;

    @Inject
    public TypeDeserializer(TypeManager typeManager)
    {
      super(Type.class);
      this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected Type _deserialize(String value, DeserializationContext context)
    {
      Type type = typeManager.getType(parseTypeSignature(value));
      checkArgument(type != null, "Unknown type %s", value);
      return type;
    }
  }
}

