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

import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import javax.validation.constraints.Size;

import static com.google.common.collect.Iterables.transform;

import io.airlift.configuration.Config;

public class RecordServiceConnectorConfig {
  private static final int RECORDSERVICE_PLANNER_DEFAULT_PORT = 12050;

  /**
   * RecordService planner host
   */
  private Set<HostAddress> planners = ImmutableSet.of();

  @Size(min = 1)
  public Set<HostAddress> getPlanners()
  {
    return planners;
  }

  @Config("recordservice.planner.hostports")
  public RecordServiceConnectorConfig setPlanners(String plannerHostPorts)
  {
    this.planners = (plannerHostPorts == null) ? null : parsePlannerHostPorts(plannerHostPorts);
    return this;
  }

  public static ImmutableSet<HostAddress> parsePlannerHostPorts(String plannerHostPorts)
  {
    Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
    return ImmutableSet.copyOf(transform(splitter.split(plannerHostPorts),
        (value -> HostAddress.fromString(value).withDefaultPort(RECORDSERVICE_PLANNER_DEFAULT_PORT))));
  }

}
