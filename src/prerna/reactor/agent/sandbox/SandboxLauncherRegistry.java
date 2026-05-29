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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Static holder that picks a {@link SandboxLauncher} for the current host.
 *
 * <p>Throws {@link SandboxUnavailableException} from {@link #get()} when no
 * backend is available so callers fail closed without a separate guard.
 * Successful resolution is cached; failures are not (re-detected each call,
 * which is cheap).
 */
public final class SandboxLauncherRegistry {

    private static final Logger logger = LogManager.getLogger(SandboxLauncherRegistry.class);

    private static volatile SandboxLauncher cached;

    private SandboxLauncherRegistry() {}

    public static SandboxLauncher get() {
        SandboxLauncher c = cached;
        if (c != null) return c;
        synchronized (SandboxLauncherRegistry.class) {
            if (cached == null) {
                cached = detect();
            }
            return cached;
        }
    }

    /**
     * Non-throwing variant of {@link #get()} for callers that need to branch on
     * availability before committing to a sandboxed code path.
     */
    public static boolean isAvailable() {
        try {
            get();
            return true;
        } catch (SandboxUnavailableException e) {
            return false;
        }
    }

    /** For tests; clears the cached launcher so a different platform can be simulated. */
    public static synchronized void reset() {
        cached = null;
    }

    /** For tests / explicit overrides. */
    public static synchronized void setOverride(SandboxLauncher override) {
        cached = override;
    }

    private static SandboxLauncher detect() {
        Platform p = Platform.current();
        SandboxLauncher backend = null;
        if (p == Platform.LINUX) {
            backend = new LandlockLauncher(
                    resolveJavaExecutable(),
                    System.getProperty("java.class.path"));
        } else if (p == Platform.MACOS) {
            backend = new SandboxExecLauncher();
        }
        if (backend != null && backend.isAvailable()) {
            logger.info("Sandbox backend resolved: platform={} backend={}", p, backend.getClass().getSimpleName());
            return backend;
        }
        throw new SandboxUnavailableException(
                "AGENT_SANDBOX_ENABLE=true but no sandbox backend is available for platform " + p);
    }

    private static String resolveJavaExecutable() {
        String javaHome = System.getProperty("java.home");
        if (javaHome == null) {
            return "java";
        }
        java.nio.file.Path candidate = java.nio.file.Paths.get(javaHome, "bin", "java");
        if (java.nio.file.Files.isExecutable(candidate)) {
            return candidate.toString();
        }
        return "java";
    }
}
