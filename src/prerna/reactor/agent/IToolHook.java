/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.agent;

import java.util.Map;

// Observer-style hooks fired around a single tool invocation inside a harness turn.
// Throws are logged + swallowed by the executor so a misbehaving hook cannot abort the agent.
// Hooks live on AgentConfig.getToolHooks(), populated by AgentConfigLoader from
// CONFIG_JSON.hooks[] entries whose resolved class implements this interface.
public interface IToolHook extends IAgentHook {

    // Fires just before the tool is dispatched. params is a defensive copy.
    default void beforeTool(AgentRunContext ctx,
                            String toolName,
                            String toolCallId,
                            Map<String, Object> params,
                            int iteration) throws Exception {}

    // Fires after dispatch returns (success or error). resultContent is the raw tool output
    // before it is handed back to the model; durationMs is wall time of the dispatch itself.
    default void afterTool(AgentRunContext ctx,
                           String toolName,
                           String toolCallId,
                           Map<String, Object> params,
                           String resultContent,
                           long durationMs,
                           boolean success,
                           int iteration) throws Exception {}
}
