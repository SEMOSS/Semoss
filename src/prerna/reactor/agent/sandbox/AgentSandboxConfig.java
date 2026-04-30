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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

/**
 * DIHelper-backed defaults for {@link SandboxPolicy} and convenience
 * constructors for the baseline policy every agent run gets.
 *
 * <p>Config keys (all read from {@code RDF_Map.prop} via {@link Utility#getDIHelperProperty(String)}):
 * <table>
 * <tr><th>Key</th><th>Meaning</th><th>Default</th></tr>
 * <tr><td>{@code AGENT_SANDBOX_ENFORCE}</td>
 *     <td>One of {@code ENFORCE}, {@code PERMISSIVE}, {@code DISABLED}</td>
 *     <td>{@code ENFORCE}</td></tr>
 * <tr><td>{@code AGENT_SANDBOX_DEFAULT_READS}</td>
 *     <td>Comma-separated baseline RO paths added to every policy</td>
 *     <td>OS-dependent (TLS cert dir, JRE, /usr/lib)</td></tr>
 * <tr><td>{@code AGENT_SANDBOX_LOOPBACK_NETWORK}</td>
 *     <td>{@code true} to allow localhost MCP callbacks</td>
 *     <td>{@code true}</td></tr>
 * </table>
 */
public final class AgentSandboxConfig {

    private static final Logger logger = LogManager.getLogger(AgentSandboxConfig.class);

    public static final String CFG_ENFORCE          = "AGENT_SANDBOX_ENFORCE";
    public static final String CFG_DEFAULT_READS    = "AGENT_SANDBOX_DEFAULT_READS";
    public static final String CFG_LOOPBACK_NETWORK = "AGENT_SANDBOX_LOOPBACK_NETWORK";

    private AgentSandboxConfig() {}

    public static EnforcementMode resolveEnforcement() {
        String raw = Utility.getDIHelperProperty(CFG_ENFORCE);
        if (raw == null || raw.trim().isEmpty()) {
            return EnforcementMode.ENFORCE;
        }
        try {
            return EnforcementMode.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid {} value '{}' — defaulting to ENFORCE", CFG_ENFORCE, raw);
            return EnforcementMode.ENFORCE;
        }
    }

    public static boolean resolveLoopbackNetwork() {
        String raw = Utility.getDIHelperProperty(CFG_LOOPBACK_NETWORK);
        if (raw == null || raw.trim().isEmpty()) {
            return true;
        }
        return Boolean.parseBoolean(raw.trim());
    }

    /**
     * OS-aware baseline read-only paths the agent binary always needs:
     * loader / TLS certs / system libraries. Overridable via
     * {@link #CFG_DEFAULT_READS} (CSV of absolute paths).
     */
    public static List<String> resolveDefaultReads() {
        String raw = Utility.getDIHelperProperty(CFG_DEFAULT_READS);
        if (raw != null && !raw.trim().isEmpty()) {
            List<String> out = new ArrayList<>();
            for (String p : raw.split(",")) {
                String trimmed = p.trim();
                if (!trimmed.isEmpty()) out.add(trimmed);
            }
            return out;
        }
        Platform p = Platform.current();
        if (p == Platform.LINUX) {
            return Arrays.asList(
                    "/lib",
                    "/lib64",
                    "/usr/lib",
                    "/usr/lib64",
                    "/usr/bin",
                    "/bin",
                    "/etc/ssl",
                    "/etc/resolv.conf",
                    System.getProperty("java.home")
            );
        }
        if (p == Platform.MACOS) {
            return Arrays.asList(
                    "/usr/lib",
                    "/usr/bin",
                    "/System",
                    "/Library",
                    "/private/etc",
                    System.getProperty("java.home")
            );
        }
        return new ArrayList<>();
    }

    /**
     * Build a baseline {@link SandboxPolicy} appropriate for an agent run.
     *
     * @param roomFolderPath       the per-room scratch/session directory (RW)
     * @param workingDirectory     the filePath/project slice the agent is editing (RW); nullable
     * @param targetBinaryPath     absolute path to the agent binary being wrapped (its parent dir is RO);
     *                             nullable — callers may pass {@code null} when the path isn't known yet
     */
    public static SandboxPolicy defaultPolicy(String roomFolderPath,
                                              String workingDirectory,
                                              String targetBinaryPath) {
        SandboxPolicyBuilder b = SandboxPolicy.builder()
                .withEnforcement(resolveEnforcement())
                .withLoopbackNetwork(resolveLoopbackNetwork());

        if (roomFolderPath != null && !roomFolderPath.trim().isEmpty()) {
            b.withReadWrite(roomFolderPath);
        }
        if (workingDirectory != null && !workingDirectory.trim().isEmpty()) {
            b.withReadWrite(workingDirectory);
        }
        if (targetBinaryPath != null && !targetBinaryPath.trim().isEmpty()) {
            File parent = new File(targetBinaryPath).getParentFile();
            if (parent != null) {
                b.withRead(parent.getAbsolutePath());
            }
        }
        for (String ro : resolveDefaultReads()) {
            try {
                b.withRead(ro);
            } catch (IllegalArgumentException ignored) {
                // Skip entries that aren't absolute or don't parse; the user's CSV
                // might have bad rows and we shouldn't break the whole run for one.
            }
        }
        return b.build();
    }

    /**
     * Build the sandbox policy that should actually be applied to a run:
     * always start from {@link #defaultPolicy(String, String, String)} (room
     * folder + working dir + binary parent + DIHelper default reads), and
     * overlay any pixel-supplied {@code override} on top — paths are unioned
     * (RW wins over RO via the builder's dedupe), and the override's
     * enforcement mode wins when present.
     *
     * <p>This is the entry point harnesses should call instead of branching on
     * {@code ctx.getSandboxPolicy()} directly — otherwise pixel-level
     * {@code sandbox_writes}/{@code sandbox_reads} would replace the defaults
     * and leave the agent binary unreadable inside the sandbox.
     */
    public static SandboxPolicy buildEffectivePolicy(String roomFolderPath,
                                                     String workingDirectory,
                                                     String targetBinaryPath,
                                                     SandboxPolicy override) {
        SandboxPolicy base = defaultPolicy(roomFolderPath, workingDirectory, targetBinaryPath);
        if (override == null) {
            return base;
        }
        SandboxPolicyBuilder b = SandboxPolicy.builder()
                .withEnforcement(override.getEnforcement() != null ? override.getEnforcement() : base.getEnforcement())
                .withLoopbackNetwork(base.isLoopbackNetwork());
        base.getTmpDir().ifPresent(b::withTmpDir);
        for (AllowedPath ap : base.getAllowedPaths()) {
            if (ap.getMode() == AccessMode.RW) {
                b.withReadWrite(ap.getPath());
            } else {
                b.withRead(ap.getPath());
            }
        }
        for (AllowedPath ap : override.getAllowedPaths()) {
            if (ap.getMode() == AccessMode.RW) {
                b.withReadWrite(ap.getPath());
            } else {
                b.withRead(ap.getPath());
            }
        }
        return b.build();
    }
}
