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

import org.json.JSONObject;

/**
 * Marker for agent hooks. Sub-interfaces:
 * <ul>
 *   <li>{@link IAgentRunHook} fires at run-level lifecycle points
 *       (room creation, init, de-init, before/after run)</li>
 *   <li>{@link IToolHook} fires per tool call</li>
 * </ul>
 * A single class may implement both; the loader classifies by
 * {@code instanceof} at resolve time.
 *
 * <p>Hooks may optionally read per-instance configuration from the
 * {@code WORKSPACE.CONFIG_JSON.hooks[]} entry that produced them by
 * overriding {@link #configure(JSONObject)}. The loader calls
 * {@code configure(spec)} immediately after constructing the hook from
 * the registry factory. Hooks that don't need per-instance config (e.g.
 * {@link prerna.reactor.agent.hooks.LoggingToolHook}) can ignore it;
 * config-bearing hooks (e.g. {@link prerna.reactor.agent.hooks.PixelReactorHook})
 * override to extract their fields.
 */
public interface IAgentHook {

    /**
     * Called once by {@code AgentConfigLoader} after the hook is
     * constructed, with the full JSON spec for this hook entry
     * (including {@code "kind"} and any kind-specific fields). Throw
     * to signal a misconfigured spec — the loader logs and skips that
     * hook. Default: no-op.
     */
    default void configure(JSONObject spec) {}
}
