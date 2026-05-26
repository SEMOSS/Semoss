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

/**
 * Strategy for translating a {@link SandboxPolicy} into an OS-specific spawn plan.
 *
 * <p>One implementation per platform:
 * <ul>
 *   <li>{@link LandlockLauncher} — Linux, landlock LSM via JNA
 *   <li>{@link SandboxExecLauncher} — macOS, {@code sandbox-exec} + Seatbelt profile
 * </ul>
 *
 * <p>If neither backend is available on the host,
 * {@link SandboxLauncherRegistry#get()} throws {@link SandboxUnavailableException}
 * — there is no fallback launcher.
 */
public interface SandboxLauncher {

    /**
     * @return the platform this launcher handles, for registry wiring.
     */
    Platform getPlatform();

    /**
     * @return {@code true} if the backend can actually enforce the policy on
     *         the current host (e.g. kernel ≥ 5.13 for landlock, {@code
     *         sandbox-exec} present on Mac). Callers consult this before
     *         {@link #plan(SandboxPolicy, String, String[])} to decide whether
     *         to honor {@link EnforcementMode#ENFORCE} or fall back.
     */
    boolean isAvailable();

    /**
     * Build a spawn plan.
     *
     * @param policy       allowlist to apply. Must not be {@code null}.
     * @param targetCliPath absolute path to the real agent binary we want the
     *                     sandbox to exec (e.g. {@code /usr/local/bin/claude}
     *                     or the Copilot CLI). Must be absolute.
     * @param targetCliArgs arguments the SDK wants to pass to the target binary.
     *                     These are appended by callers after {@code cliArgs}
     *                     from the returned plan; for SDK integrations that
     *                     accept {@code cliArgs} separately, pass the same array
     *                     here so the plan can verify invariants.
     * @return a {@link SandboxLaunchPlan} describing {@code cliPath}, prefix
     *         {@code cliArgs}, env additions and removals.
     *
     * @throws SandboxUnavailableException if enforcement is required but the
     *         backend cannot be applied.
     */
    SandboxLaunchPlan plan(SandboxPolicy policy, String targetCliPath, String[] targetCliArgs);
}
