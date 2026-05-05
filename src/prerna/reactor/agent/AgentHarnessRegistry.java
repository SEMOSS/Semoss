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
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Static registry for {@link IAgentHarness} factories.
 *
 * <p>Each call to {@link #get} or {@link #getOrDefault} returns a <b>new instance</b>
 * of the requested harness. This ensures thread safety — harness instances are never
 * shared across concurrent requests.
 *
 * <p>Built-in harnesses registered at class-load time:
 * <ul>
 *   <li>{@code "room_loop"} → {@link RoomAgentHarness}
 *   <li>{@code "claude_code"} → {@link ClaudeCodeAgentHarness}
 *   <li>{@code "github_copilot"} → {@link GitHubCopilotAgentHarness}
 *   <li>{@code "github_copilot_py"} → {@link GitHubCopilotPyAgentHarness}
 *   <li>{@code "orchestrator"} → {@link OrchestratorAgentHarness}
 * </ul>
 *
 * <p>Custom harnesses can be registered at application startup via {@link #register}.
 * The default harness (used when the requested name is unknown) is {@code "room_loop"}.
 */
public final class AgentHarnessRegistry {

    private static final Logger logger = LogManager.getLogger(AgentHarnessRegistry.class);

    public static final String DEFAULT_HARNESS = "room_loop";

    private static final Map<String, Supplier<? extends IAgentHarness>> REGISTRY;

    static {
        Map<String, Supplier<? extends IAgentHarness>> m = new HashMap<>();
        m.put(RoomAgentHarness.NAME,            RoomAgentHarness::new);
        m.put(ClaudeCodeAgentHarness.NAME,      ClaudeCodeAgentHarness::new);
        m.put(GitHubCopilotAgentHarness.NAME,   GitHubCopilotAgentHarness::new);
        m.put(GitHubCopilotPyAgentHarness.NAME, GitHubCopilotPyAgentHarness::new);
        m.put(OrchestratorAgentHarness.NAME,    OrchestratorAgentHarness::new);
        REGISTRY = Collections.synchronizedMap(m);
    }

    private AgentHarnessRegistry() { /* static utility */ }

    /**
     * Register a custom harness factory. Overwrites any existing factory with the same name.
     *
     * @param name    registry key (case-sensitive)
     * @param factory supplier that creates new harness instances; must not be null
     */
    public static void register(String name, Supplier<? extends IAgentHarness> factory) {
        if (name == null || factory == null) throw new IllegalArgumentException("name and factory must not be null");
        REGISTRY.put(name, factory);
        logger.info("AgentHarnessRegistry: registered harness factory '{}'", name);
    }

    /**
     * Register a custom harness. Convenience overload — creates a factory from the harness's class.
     *
     * @param harness implementation to register; must not be null
     */
    public static void register(IAgentHarness harness) {
        if (harness == null) throw new IllegalArgumentException("harness must not be null");
        String name = harness.getName();
        // Create a factory that instantiates via reflection for backward compatibility
        Class<? extends IAgentHarness> clazz = harness.getClass();
        REGISTRY.put(name, () -> {
            try {
                return clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate harness: " + clazz.getName(), e);
            }
        });
        logger.info("AgentHarnessRegistry: registered harness '{}'", name);
    }

    /**
     * Returns a new instance of the harness registered under {@code name}, or {@code null} if not found.
     *
     * @param name registry key (case-sensitive)
     */
    public static IAgentHarness get(String name) {
        Supplier<? extends IAgentHarness> factory = REGISTRY.get(name);
        return factory != null ? factory.get() : null;
    }

    /**
     * Returns a new instance of the harness registered under {@code name}.
     * Falls back to the {@value #DEFAULT_HARNESS} harness if {@code name} is null,
     * blank, or unrecognised.
     *
     * @param name registry key; may be null or empty
     */
    public static IAgentHarness getOrDefault(String name) {
        if (name != null && !name.trim().isEmpty()) {
            Supplier<? extends IAgentHarness> factory = REGISTRY.get(name.trim());
            if (factory != null) return factory.get();
            logger.warn("AgentHarnessRegistry: unknown harness '{}' — falling back to '{}'",
                    name, DEFAULT_HARNESS);
        }
        return REGISTRY.get(DEFAULT_HARNESS).get();
    }
}
