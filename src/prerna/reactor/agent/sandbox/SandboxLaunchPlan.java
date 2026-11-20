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
package prerna.reactor.agent.sandbox;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of {@link SandboxLauncher#plan(SandboxPolicy, String, String[])}.
 *
 * <p>Encodes how a callsite should spawn the real agent binary under the
 * sandbox: the final argv to hand to a {@code ProcessBuilder} (or to an
 * SDK that takes {@code cliPath} + {@code cliArgs}), plus environment
 * variables the launcher expects to read (the policy JSON path, the real
 * target, etc.).
 */
public final class SandboxLaunchPlan {

    private final String             cliPath;
    private final List<String>       cliArgs;
    private final Map<String, String> environmentAdditions;
    private final List<String>       environmentRemovals;
    private final boolean            sandboxed;
    private final String             backend;

    public SandboxLaunchPlan(String cliPath,
                             List<String> cliArgs,
                             Map<String, String> environmentAdditions,
                             List<String> environmentRemovals,
                             boolean sandboxed,
                             String backend) {
        this.cliPath              = cliPath;
        this.cliArgs              = Collections.unmodifiableList(cliArgs);
        this.environmentAdditions = Collections.unmodifiableMap(new LinkedHashMap<>(environmentAdditions));
        this.environmentRemovals  = Collections.unmodifiableList(environmentRemovals);
        this.sandboxed            = sandboxed;
        this.backend              = backend;
    }

    /** The executable to invoke (argv[0] for a {@link ProcessBuilder}). */
    public String getCliPath() {
        return cliPath;
    }

    /**
     * Arguments prepended before the target binary's own arguments.
     * SDKs that accept a {@code cliArgs} array (GitHub Copilot) take this
     * verbatim; direct {@code ProcessBuilder} callers should concatenate
     * {@code [cliPath, ...cliArgs, ...targetArgs]}.
     */
    public List<String> getCliArgs() {
        return cliArgs;
    }

    /** Env vars to set on the spawned process (e.g. {@code SEMOSS_SANDBOX_POLICY_JSON}). */
    public Map<String, String> getEnvironmentAdditions() {
        return environmentAdditions;
    }

    /**
     * Env vars to drop from the inherited environment before spawn
     * (e.g. {@code LD_PRELOAD}, {@code LD_LIBRARY_PATH}).
     */
    public List<String> getEnvironmentRemovals() {
        return environmentRemovals;
    }

    /** {@code false} when the launcher is a no-op (permissive / disabled fallback). */
    public boolean isSandboxed() {
        return sandboxed;
    }

    /** Human-readable backend name for logs: {@code "landlock"}, {@code "sandbox-exec"}, {@code "noop"}. */
    public String getBackend() {
        return backend;
    }
}
