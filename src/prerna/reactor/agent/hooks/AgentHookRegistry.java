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
package prerna.reactor.agent.hooks;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.agent.IAgentHook;
import prerna.reactor.agent.IAgentRunHook;

/**
 * Static registry for {@link IAgentHook} kinds. One source of truth for both
 * the validate-on-write path ({@code SetWorkspaceHooksReactor}) and the
 * resolve-on-read path ({@code AgentConfigLoader.resolveHook}).
 *
 * <p>Built-in hooks registered at class-load time:
 * <ul>
 *   <li>{@code "git_commit"} -> {@link GitCommitAgentHook} - runs
 *       {@code git add . && git commit} after each successful agent run.</li>
 * </ul>
 *
 * <p>Custom hooks can be registered at application startup via
 * {@link #register(String, Supplier)}. Each entry is a factory ({@link Supplier})
 * so the registry can produce fresh instances per run if a hook ever needs
 * per-call state - today's built-in hook is stateless.
 *
 * <p>Mirrors {@code AgentHarnessRegistry} in shape; the only meaningful
 * difference is the factory-based registration so hooks can be stateful.
 */
public final class AgentHookRegistry {

    private static final Logger logger = LogManager.getLogger(AgentHookRegistry.class);

    private static final Map<String, Supplier<? extends IAgentHook>> REGISTRY;

    /** JSON {@code kind} value for {@link GitCommitAgentHook}. */
    public static final String GIT_COMMIT = "git_commit";

    /** JSON {@code kind} value for {@link LoggingToolHook}. */
    public static final String LOG_TOOLS = "log_tools";

    static {
        Map<String, Supplier<? extends IAgentHook>> m = new HashMap<>();
        m.put(GIT_COMMIT, GitCommitAgentHook::new);
        m.put(LOG_TOOLS,  LoggingToolHook::new);
        REGISTRY = Collections.synchronizedMap(m);
    }

    private AgentHookRegistry() { /* static utility */ }

    /**
     * Register (or overwrite) a hook factory under the given kind. Intended
     * for application-startup wiring; the built-ins are already populated.
     *
     * @param kind    JSON {@code kind} string clients write into CONFIG_JSON.hooks[]
     * @param factory zero-arg supplier producing a fresh hook instance per call
     */
    public static void register(String kind, Supplier<? extends IAgentHook> factory) {
        if (kind == null || kind.trim().isEmpty()) {
            throw new IllegalArgumentException("kind must not be null or empty");
        }
        if (factory == null) {
            throw new IllegalArgumentException("factory must not be null");
        }
        REGISTRY.put(kind, factory);
        logger.info("AgentHookRegistry: registered hook '{}'", kind);
    }

    /**
     * Returns a fresh hook instance for the given kind, or {@code null} when
     * the kind is unknown. Callers should log + skip on null rather than
     * throwing - preserves forward-compat against newer CONFIG_JSON written
     * by a future server version.
     */
    public static IAgentHook resolve(String kind) {
        if (kind == null) return null;
        Supplier<? extends IAgentHook> factory = REGISTRY.get(kind);
        return factory == null ? null : factory.get();
    }

    /** {@code true} when a factory is registered under {@code kind}. */
    public static boolean isKnown(String kind) {
        return kind != null && REGISTRY.containsKey(kind);
    }

    /**
     * Snapshot of currently-known kinds. Stable order isn't guaranteed; use
     * for error messages and FE option lists, not for protocol identity.
     */
    public static Set<String> knownKinds() {
        synchronized (REGISTRY) {
            return Collections.unmodifiableSet(new LinkedHashSet<>(REGISTRY.keySet()));
        }
    }
}
