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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SandboxExecLauncherUnitTests {

    @Test
    void planEmitsWrapperAndSbProfile(@TempDir Path tmp) throws Exception {
        SandboxExecLauncher launcher = new SandboxExecLauncher();
        SandboxPolicy policy = SandboxPolicy.builder()
                .withReadWrite(tmp.toString())
                .withRead("/usr/lib")
                .withTmpDir(tmp)
                .withLoopbackNetwork(true)
                .build();

        SandboxLaunchPlan plan = launcher.plan(policy, "/usr/bin/claude", new String[0]);

        assertEquals("sandbox-exec", plan.getBackend());
        assertTrue(plan.getCliPath().endsWith(".sh"));
        assertTrue(plan.getCliArgs().isEmpty());

        String profilePath = plan.getEnvironmentAdditions().get("SEMOSS_SANDBOX_PROFILE_FILE");
        assertTrue(profilePath != null && profilePath.endsWith(".sb"),
                "expected .sb profile path, got " + profilePath);

        String body = Files.readString(Path.of(profilePath));
        assertTrue(body.contains("(deny default)"), "profile must be deny-default");
        assertTrue(body.contains("(subpath \"" + tmp.toString() + "\")"),
                "profile must allow the RW subpath");
        assertTrue(body.contains("(subpath \"/usr/lib\")"),
                "profile must allow the RO subpath");
        assertTrue(body.contains("localhost"), "loopback network should be allowed");

        assertEquals("/usr/bin/claude",
                plan.getEnvironmentAdditions().get(SandboxLauncherMain.ENV_TARGET_CLI));
        assertTrue(plan.getEnvironmentRemovals().contains("DYLD_INSERT_LIBRARIES"));
    }
}
