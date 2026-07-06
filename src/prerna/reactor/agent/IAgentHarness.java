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

/**
 * Pluggable harness interface for the generic agent loop.
 *
 * <p>Implementations receive a resolved {@link AgentRunContext} and drive the
 * model/tool loop to a final response. Built-ins are registered through
 * {@link AgentHarnessRegistry}, and custom harnesses can be added at startup.
 */
public interface IAgentHarness {

    /**
     * Unique registry key for this harness (e.g. {@code "room_loop"}, {@code "claude_code"}).
     * Must be stable across JVM restarts.
     */
    String getName();

    /**
     * @return true when this harness can attach current-turn media to the initial
     *         user message.
     */
    default boolean supportsMediaInput() {
        return false;
    }

    /**
     * Execute the agentic loop and return a rich result.
     *
     * @param ctx fully-resolved context containing Room, model engine, insight, and parameters
     * @return result with final text, iteration count, and per-tool-call trace
     * @throws Exception on unrecoverable errors (callers should wrap and surface to the user)
     */
    AgentHarnessResult execute(AgentRunContext ctx) throws Exception;
}
