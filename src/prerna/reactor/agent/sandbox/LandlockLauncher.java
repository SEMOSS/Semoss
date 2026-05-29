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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.agent.sandbox.linux.Landlock;

/**
 * Linux sandbox backend (Landlock LSM, kernel 5.13+). The spawn plan hands the SDK a
 * wrapper script that re-execs through {@link SandboxLauncherMain}, which applies the
 * ruleset via JNA and {@code execvp()}s the target binary. Adds ~500ms startup overhead.
 */
public final class LandlockLauncher implements SandboxLauncher {

    private static final Logger logger = LogManager.getLogger(LandlockLauncher.class);

    private final String javaExecutable;
    private final String bootstrapClasspath;

    public LandlockLauncher(String javaExecutable, String bootstrapClasspath) {
        this.javaExecutable     = javaExecutable;
        this.bootstrapClasspath = bootstrapClasspath;
    }

    @Override
    public Platform getPlatform() {
        return Platform.LINUX;
    }

    @Override
    public boolean isAvailable() {
        int ver = Landlock.probeAbiVersion();
        if (ver < 0) {
            logger.warn("Landlock unavailable on this host (kernel < 5.13 or landlock disabled); "
                    + "Command reactor sandbox enforcement will refuse to run");
            return false;
        }
        if (ver < 4) {
            // ABI 4 introduced LANDLOCK_ACCESS_NET_CONNECT_TCP / *_BIND_TCP. Below that,
            // the network field on SandboxPolicy is advisory only.
            logger.warn("Landlock ABI v{} detected — network restrictions are NOT enforced "
                    + "below ABI v4 (Linux 6.7+). File-system confinement is active.", ver);
        } else {
            logger.info("Landlock ABI v{} detected (file + network confinement available)", ver);
        }
        return true;
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

        Path tmpDir = policy.getTmpDir().orElse(Paths.get(System.getProperty("java.io.tmpdir")));
        Path policyFile = PolicyJson.writeToFile(policy, tmpDir, "semoss-sandbox-policy-");
        Path wrapper   = LauncherScriptWriter.write(tmpDir);

        Map<String, String> envAdd = new LinkedHashMap<>();
        envAdd.put(SandboxLauncherMain.ENV_POLICY_FILE,   policyFile.toString());
        envAdd.put(SandboxLauncherMain.ENV_TARGET_CLI,    target.toString());
        envAdd.put(SandboxLauncherMain.ENV_BACKEND,       "landlock");
        envAdd.put("SEMOSS_SANDBOX_JAVA_BIN",             javaExecutable);
        envAdd.put("SEMOSS_SANDBOX_CLASSPATH",            bootstrapClasspath);

        // Scrub dangerous inheritable env that could bypass the sandbox.
        List<String> envRemove = Arrays.asList(
                "LD_PRELOAD",
                "LD_AUDIT",
                "LD_LIBRARY_PATH",
                "PYTHONINSPECT"
        );

        return new SandboxLaunchPlan(
                wrapper.toString(),
                Collections.emptyList(),
                envAdd,
                envRemove,
                true,
                "landlock");
    }
}
