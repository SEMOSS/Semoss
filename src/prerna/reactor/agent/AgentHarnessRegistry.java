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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.agent.runtime.SemossAgentHarness;

/**
 * Static registry for {@link IAgentHarness} implementations.
 *
 * <p>Built-in harnesses registered at class-load time:
 * <ul>
 *   <li>{@code "room_loop"} -> {@link RoomAgentHarness} - deprecated legacy SEMOSS room loop
 *   <li>{@code "semoss"}    -> {@link SemossAgentHarness} - SEMOSS-native canonical harness
 *   <li>{@code "claude_code"} -> {@link ClaudeCodeAgentHarness}
 *   <li>{@code "github_copilot_py"} -> {@link GitHubCopilotPyAgentHarness}
 * </ul>
 *
 * <p>Custom harnesses can be registered at application startup via {@link #register}.
 * The default harness (used when the requested name is unknown) is {@code "semoss"}.
 */
public final class AgentHarnessRegistry {

    private static final Logger logger = LogManager.getLogger(AgentHarnessRegistry.class);

    public static final String DEFAULT_HARNESS = "semoss";

    private static final Map<String, IAgentHarness> REGISTRY;

    static {
        Map<String, IAgentHarness> m = new HashMap<>();
        IAgentHarness roomLoop        = new RoomAgentHarness();
        IAgentHarness semoss          = new SemossAgentHarness();
        IAgentHarness claudeCode      = new ClaudeCodeAgentHarness();
        IAgentHarness githubCopilotPy = new GitHubCopilotPyAgentHarness();
        m.put(roomLoop.getName(),        roomLoop);
        m.put(semoss.getName(),          semoss);
        m.put(claudeCode.getName(),      claudeCode);
        m.put(githubCopilotPy.getName(), githubCopilotPy);
        REGISTRY = Collections.synchronizedMap(m);
    }

    private AgentHarnessRegistry() { /* static utility */ }

    // Public API
    /**
     * Register a custom harness. Overwrites any existing harness with the same name.
     *
     * @param harness implementation to register; must not be null
     */
    public static void register(IAgentHarness harness) {
        if (harness == null) throw new IllegalArgumentException("harness must not be null");
        REGISTRY.put(harness.getName(), harness);
        logger.info("AgentHarnessRegistry: registered harness '{}'", harness.getName());
    }

    /**
     * Returns the harness registered under {@code name}, or {@code null} if not found.
     *
     * @param name registry key (case-sensitive)
     */
    public static IAgentHarness get(String name) {
        return REGISTRY.get(name);
    }

    /**
     * Returns the harness registered under {@code name}.
     * Falls back to the {@value #DEFAULT_HARNESS} harness if {@code name} is null,
     * blank, or unrecognised.
     *
     * @param name registry key; may be null or empty
     */
    public static IAgentHarness getOrDefault(String name) {
        if (name != null && !name.trim().isEmpty()) {
            IAgentHarness h = REGISTRY.get(name.trim());
            if (h != null) return h;
            logger.warn("AgentHarnessRegistry: unknown harness '{}' - falling back to '{}'",
                    name, DEFAULT_HARNESS);
        }
        return REGISTRY.get(DEFAULT_HARNESS);
    }
}
