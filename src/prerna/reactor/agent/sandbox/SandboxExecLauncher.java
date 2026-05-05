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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * macOS {@link SandboxLauncher} backed by Apple Seatbelt ({@code sandbox-exec}).
 *
 * <p>Generates a {@code .sb} profile from the {@link SandboxPolicy} and a
 * tiny shell wrapper (see {@link LauncherScriptWriter}) that invokes
 * {@code /usr/bin/sandbox-exec -f &lt;profile&gt; &lt;target&gt; "$@"}.
 * The shell wrapper path is what callers hand to an SDK's {@code cliPath}.
 */
public final class SandboxExecLauncher implements SandboxLauncher {

    private static final Logger logger = LogManager.getLogger(SandboxExecLauncher.class);

    private static final Path SANDBOX_EXEC = Paths.get("/usr/bin/sandbox-exec");

    @Override
    public Platform getPlatform() {
        return Platform.MACOS;
    }

    @Override
    public boolean isAvailable() {
        boolean ok = Files.isExecutable(SANDBOX_EXEC);
        if (!ok) {
            logger.info("sandbox-exec not found at {}", SANDBOX_EXEC);
        }
        return ok;
    }

    @Override
    public SandboxLaunchPlan plan(SandboxPolicy policy, String targetCliPath, String[] targetCliArgs) {
        if (policy == null) {
            throw new IllegalArgumentException("policy is required");
        }
        if (targetCliPath == null || targetCliPath.trim().isEmpty()) {
            throw new IllegalArgumentException("targetCliPath is required");
        }
        Path target = Paths.get(targetCliPath);
        if (!target.isAbsolute()) {
            throw new IllegalArgumentException("targetCliPath must be absolute: " + targetCliPath);
        }

        Path tmpDir  = policy.getTmpDir().orElse(Paths.get(System.getProperty("java.io.tmpdir")));
        Path profile = writeProfile(policy, tmpDir);
        Path wrapper = LauncherScriptWriter.write(tmpDir);

        Map<String, String> envAdd = new LinkedHashMap<>();
        envAdd.put(SandboxLauncherMain.ENV_TARGET_CLI,   target.toString());
        envAdd.put(SandboxLauncherMain.ENV_BACKEND,      "sandbox-exec");
        envAdd.put(SandboxLauncherMain.ENV_PROFILE_FILE, profile.toString());

        List<String> envRemove = Arrays.asList("DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH");

        return new SandboxLaunchPlan(
                wrapper.toString(),
                Collections.emptyList(),
                envAdd,
                envRemove,
                true,
                "sandbox-exec");
    }

    private Path writeProfile(SandboxPolicy policy, Path tmpDir) {
        StringBuilder sb = new StringBuilder();
        sb.append("(version 1)\n");
        sb.append("(deny default)\n");
        // Kernel / process / IPC boilerplate required by any Node.js or JVM
        // subprocess.  file-read-metadata is global so dyld can stat() paths at
        // startup; /dev/urandom must be readable or OpenSSL aborts during PRNG seed.
        sb.append("(allow process-fork)\n");
        sb.append("(allow process-exec*)\n");
        sb.append("(allow process-info*)\n");
        sb.append("(allow signal)\n");
        sb.append("(allow mach-lookup)\n");
        sb.append("(allow mach-task-name)\n");
        sb.append("(allow iokit-open)\n");
        sb.append("(allow iokit-get-properties)\n");
        sb.append("(allow sysctl-read)\n");
        sb.append("(allow ipc-posix-shm)\n");
        sb.append("(allow system-fsctl)\n");
        sb.append("(allow file-read-metadata)\n");
        sb.append("(allow file-read-data (literal \"/dev/urandom\"))\n");
        sb.append("(allow file-read-data (literal \"/dev/random\"))\n");
        sb.append("(allow file-read-data (literal \"/dev/null\"))\n");
        sb.append("(allow file-write* (literal \"/dev/null\"))\n");
        sb.append("(allow file-write* (literal \"/dev/tty\"))\n");
        sb.append("(allow file-write* (literal \"/dev/dtracehelper\"))\n");

        if (policy.isLoopbackNetwork()) {
            sb.append("(allow network* (remote ip \"localhost:*\"))\n");
            sb.append("(allow network* (local ip))\n");
        } else {
            sb.append("(deny network*)\n");
        }

        // Reads are broadly allowed; blocks below override via more-specific deny
        // rules, and the per-path allow rules in the next loop carve back out any
        // RW/RO entry that falls inside a blocked ancestor.
        sb.append("(allow file-read-data)\n");

        for (Path blocked : policy.getBlockedPaths()) {
            appendSubpathRule(sb, "deny file-read-data", blocked.toString());
        }

        for (AllowedPath ap : policy.getAllowedPaths()) {
            String pathStr = ap.getPath().toString();
            appendSubpathRule(sb, "allow file-read-data", pathStr);
            if (ap.getMode() == AccessMode.RW) {
                appendSubpathRule(sb, "allow file-write* file-ioctl", pathStr);
            }
        }
        policy.getTmpDir().ifPresent(p -> {
            String pathStr = p.toString();
            appendSubpathRule(sb, "allow file-read-data", pathStr);
            appendSubpathRule(sb, "allow file-write* file-ioctl", pathStr);
        });

        try {
            Files.createDirectories(tmpDir);
            Path file = Files.createTempFile(tmpDir, "semoss-sandbox-", ".sb");
            file.toFile().deleteOnExit();
            Files.writeString(file, sb.toString(),
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            return file.toAbsolutePath();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write sandbox-exec profile", e);
        }
    }

    // macOS resolves /tmp -> /private/tmp and /var/... -> /private/var/... before
    // consulting the policy, so paths under those prefixes need a mirrored rule.
    // op is the full statement verb+action, e.g. "allow file-read-data" or "deny file-read-data".
    private static void appendSubpathRule(StringBuilder sb, String op, String pathStr) {
        sb.append("(").append(op).append(" (subpath \"").append(escape(pathStr)).append("\"))\n");
        if (needsPrivateMirror(pathStr)) {
            sb.append("(").append(op).append(" (subpath \"/private")
              .append(escape(pathStr)).append("\"))\n");
        }
    }

    private static boolean needsPrivateMirror(String pathStr) {
        return pathStr.equals("/tmp") || pathStr.startsWith("/tmp/")
                || pathStr.equals("/var") || pathStr.startsWith("/var/folders/");
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
