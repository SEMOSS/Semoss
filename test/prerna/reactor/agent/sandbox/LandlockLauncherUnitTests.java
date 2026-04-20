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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LandlockLauncherUnitTests {

    @Test
    void planWiresEnvAndWrapperScript(@TempDir Path tmp) {
        LandlockLauncher launcher = new LandlockLauncher("/usr/bin/java", "/cp/semoss.jar");
        SandboxPolicy policy = SandboxPolicy.builder()
                .withReadWrite(tmp.toString())
                .withTmpDir(tmp)
                .withEnforcement(EnforcementMode.ENFORCE)
                .build();

        SandboxLaunchPlan plan = launcher.plan(policy, "/usr/local/bin/copilot", new String[0]);

        assertEquals("landlock", plan.getBackend());
        assertTrue(plan.isSandboxed());
        // cliPath should be the generated wrapper shell script
        assertTrue(plan.getCliPath().endsWith(".sh"), "expected .sh wrapper, got " + plan.getCliPath());
        assertTrue(plan.getCliArgs().isEmpty(), "landlock plan should not prepend argv");

        assertEquals("/usr/local/bin/copilot",
                plan.getEnvironmentAdditions().get(SandboxLauncherMain.ENV_TARGET_CLI));
        assertEquals("landlock",
                plan.getEnvironmentAdditions().get(SandboxLauncherMain.ENV_BACKEND));
        assertEquals("/usr/bin/java",
                plan.getEnvironmentAdditions().get("SEMOSS_SANDBOX_JAVA_BIN"));
        assertEquals("/cp/semoss.jar",
                plan.getEnvironmentAdditions().get("SEMOSS_SANDBOX_CLASSPATH"));
        assertTrue(plan.getEnvironmentRemovals().contains("LD_PRELOAD"));
    }

    @Test
    void rejectsRelativeTargetPath(@TempDir Path tmp) {
        LandlockLauncher launcher = new LandlockLauncher("/usr/bin/java", "/cp/semoss.jar");
        SandboxPolicy policy = SandboxPolicy.builder()
                .withReadWrite(tmp.toString())
                .withTmpDir(tmp)
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> launcher.plan(policy, "copilot", new String[0]));
    }

    @Test
    void rejectsNullPolicy() {
        LandlockLauncher launcher = new LandlockLauncher("/usr/bin/java", "/cp/semoss.jar");
        assertThrows(IllegalArgumentException.class,
                () -> launcher.plan(null, "/usr/bin/claude", new String[0]));
    }
}
