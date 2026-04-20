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
        envAdd.put(SandboxLauncherMain.ENV_TARGET_CLI, target.toString());
        envAdd.put(SandboxLauncherMain.ENV_BACKEND,    "sandbox-exec");
        envAdd.put("SEMOSS_SANDBOX_PROFILE_FILE",      profile.toString());

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
        // Baseline every process needs
        sb.append("(allow process-fork)\n");
        sb.append("(allow process-exec*)\n");
        sb.append("(allow signal)\n");
        sb.append("(allow mach-lookup)\n");
        sb.append("(allow sysctl-read)\n");
        sb.append("(allow ipc-posix-shm)\n");
        if (policy.isLoopbackNetwork()) {
            sb.append("(allow network* (remote ip \"localhost:*\"))\n");
            sb.append("(allow network* (local ip))\n");
        } else {
            sb.append("(deny network*)\n");
        }
        // OS-level dyld / libraries / TLS trust root
        sb.append("(allow file-read*\n");
        sb.append("  (subpath \"/usr/lib\")\n");
        sb.append("  (subpath \"/usr/share\")\n");
        sb.append("  (subpath \"/System\")\n");
        sb.append("  (subpath \"/Library\")\n");
        sb.append("  (subpath \"/private/etc\")\n");
        sb.append("  (subpath \"/private/var/db\")\n");
        sb.append(")\n");

        for (AllowedPath ap : policy.getAllowedPaths()) {
            String esc = escape(ap.getPath().toString());
            if (ap.getMode() == AccessMode.RW) {
                sb.append("(allow file-read* file-write* file-ioctl (subpath \"").append(esc).append("\"))\n");
            } else {
                sb.append("(allow file-read* (subpath \"").append(esc).append("\"))\n");
            }
        }
        policy.getTmpDir().ifPresent(p ->
                sb.append("(allow file-read* file-write* file-ioctl (subpath \"")
                  .append(escape(p.toString())).append("\"))\n"));

        try {
            Files.createDirectories(tmpDir);
            Path file = Files.createTempFile(tmpDir, "semoss-sandbox-", ".sb");
            Files.writeString(file, sb.toString(),
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            return file.toAbsolutePath();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write sandbox-exec profile", e);
        }
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
