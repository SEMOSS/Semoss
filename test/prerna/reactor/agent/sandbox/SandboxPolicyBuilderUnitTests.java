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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

public class SandboxPolicyBuilderUnitTests {

    @Test
    void buildsEmptyPolicyByDefault() {
        SandboxPolicy p = SandboxPolicy.builder().build();
        assertTrue(p.getAllowedPaths().isEmpty());
        assertEquals(EnforcementMode.ENFORCE, p.getEnforcement());
        assertTrue(p.isLoopbackNetwork());
        assertFalse(p.getTmpDir().isPresent());
    }

    @Test
    void readWriteOverridesRead() {
        SandboxPolicy p = SandboxPolicy.builder()
                .withRead("/tmp/a")
                .withReadWrite("/tmp/a")
                .build();
        assertEquals(1, p.getAllowedPaths().size());
        assertEquals(AccessMode.RW, p.getAllowedPaths().get(0).getMode());
    }

    @Test
    void readDoesNotDowngradeReadWrite() {
        SandboxPolicy p = SandboxPolicy.builder()
                .withReadWrite("/tmp/a")
                .withRead("/tmp/a")
                .build();
        assertEquals(1, p.getAllowedPaths().size());
        assertEquals(AccessMode.RW, p.getAllowedPaths().get(0).getMode());
    }

    @Test
    void normalizesDotSegments() {
        SandboxPolicy p = SandboxPolicy.builder()
                .withRead("/tmp/./foo/../bar")
                .build();
        assertEquals(1, p.getAllowedPaths().size());
        assertEquals(Paths.get("/tmp/bar"), p.getAllowedPaths().get(0).getPath());
    }

    @Test
    void rejectsEmptyPath() {
        assertThrows(IllegalArgumentException.class,
                () -> SandboxPolicy.builder().withRead("").build());
        assertThrows(IllegalArgumentException.class,
                () -> SandboxPolicy.builder().withRead("   ").build());
    }

    @Test
    void rejectsNullPath() {
        assertThrows(IllegalArgumentException.class,
                () -> SandboxPolicy.builder().withRead((String) null).build());
    }

    @Test
    void defaultWorkingDirectoryPrefersRwThenTmp() {
        SandboxPolicy p = SandboxPolicy.builder()
                .withRead("/etc/ssl")
                .withReadWrite("/tmp/room/abc")
                .withRead("/usr/lib")
                .build();
        assertEquals(Paths.get("/tmp/room/abc"), p.defaultWorkingDirectory());

        SandboxPolicy onlyRo = SandboxPolicy.builder()
                .withRead("/etc/ssl")
                .withTmpDir(Paths.get("/tmp/scratch"))
                .build();
        assertEquals(Paths.get("/tmp/scratch"), onlyRo.defaultWorkingDirectory());
    }

    @Test
    void enforcementOverrideTakesEffect() {
        SandboxPolicy p = SandboxPolicy.builder()
                .withEnforcement(EnforcementMode.PERMISSIVE)
                .build();
        assertEquals(EnforcementMode.PERMISSIVE, p.getEnforcement());
    }
}
