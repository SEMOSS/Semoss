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

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Immutable filesystem allowlist used to constrain an agent-binary run
 * (claude-code, github copilot, codex, custom harness) before it executes
 * its own tools (Bash, Read/Write, etc.).
 *
 * <p>Built via {@link #builder()}. Applied to a spawn by a
 * {@link SandboxLauncher} — typically the launcher prepends an argv shim
 * and clears dangerous env vars; the actual kernel-level restriction
 * (landlock on Linux, Seatbelt on macOS) is applied by
 * {@link SandboxLauncherMain} immediately before {@code execvp()}.
 *
 * <p>Allowlist semantics: paths outside the policy are inaccessible;
 * read-only paths block writes; there is no way to grant access to a
 * specific path under a denied ancestor (this is a hard constraint of the
 * underlying kernel primitives).
 */
public final class SandboxPolicy {

    private final List<AllowedPath>  allowedPaths;
    private final Optional<Path>     tmpDir;
    private final boolean            loopbackNetwork;
    private final EnforcementMode    enforcement;

    SandboxPolicy(List<AllowedPath> allowedPaths,
                  Optional<Path> tmpDir,
                  boolean loopbackNetwork,
                  EnforcementMode enforcement) {
        this.allowedPaths    = Collections.unmodifiableList(allowedPaths);
        this.tmpDir          = tmpDir;
        this.loopbackNetwork = loopbackNetwork;
        this.enforcement     = enforcement;
    }

    /** Paths the sandbox may access, with access level. Ordered by build order. */
    public List<AllowedPath> getAllowedPaths() {
        return allowedPaths;
    }

    /** Optional per-run scratch directory. Implies RW. */
    public Optional<Path> getTmpDir() {
        return tmpDir;
    }

    /**
     * Whether the sandbox should permit loopback (127.0.0.1/::1) connections
     * so the agent can call SEMOSS tools via MCP over localhost.
     * Currently phase 1 is filesystem-only, so this is advisory.
     */
    public boolean isLoopbackNetwork() {
        return loopbackNetwork;
    }

    public EnforcementMode getEnforcement() {
        return enforcement;
    }

    /**
     * @return the first writable path, or the tmp dir, or {@code null} if neither
     *         is present. Useful as the spawned process's working directory.
     */
    public Path defaultWorkingDirectory() {
        for (AllowedPath ap : allowedPaths) {
            if (ap.getMode() == AccessMode.RW) {
                return ap.getPath();
            }
        }
        return tmpDir.orElse(null);
    }

    public static SandboxPolicyBuilder builder() {
        return new SandboxPolicyBuilder();
    }
}
