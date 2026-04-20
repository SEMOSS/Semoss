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
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PolicyJsonUnitTests {

    @Test
    void roundTripsInMemory() {
        SandboxPolicy original = SandboxPolicy.builder()
                .withReadWrite("/tmp/room/abc")
                .withRead("/etc/ssl")
                .withLoopbackNetwork(false)
                .withEnforcement(EnforcementMode.PERMISSIVE)
                .build();

        String json = PolicyJson.toJson(original);
        SandboxPolicy decoded = PolicyJson.fromJson(json);

        List<AllowedPath> a = original.getAllowedPaths();
        List<AllowedPath> b = decoded.getAllowedPaths();
        assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(a.get(i).getPath(), b.get(i).getPath());
            assertEquals(a.get(i).getMode(), b.get(i).getMode());
        }
        assertEquals(original.isLoopbackNetwork(), decoded.isLoopbackNetwork());
        assertEquals(original.getEnforcement(), decoded.getEnforcement());
    }

    @Test
    void writeToFileIsReadable(@TempDir Path tmp) throws Exception {
        SandboxPolicy original = SandboxPolicy.builder()
                .withReadWrite("/tmp/x")
                .withRead("/usr/lib")
                .build();
        Path file = PolicyJson.writeToFile(original, tmp, "t-");
        assertTrue(Files.exists(file));
        SandboxPolicy decoded = PolicyJson.fromFile(file);
        assertEquals(2, decoded.getAllowedPaths().size());
        assertEquals(Paths.get("/tmp/x"), decoded.getAllowedPaths().get(0).getPath());
        assertEquals(AccessMode.RW, decoded.getAllowedPaths().get(0).getMode());
    }

    @Test
    void tmpDirSurvivesRoundTrip() {
        SandboxPolicy original = SandboxPolicy.builder()
                .withTmpDir(Paths.get("/tmp/scratch/xyz"))
                .build();
        SandboxPolicy decoded = PolicyJson.fromJson(PolicyJson.toJson(original));
        assertTrue(decoded.getTmpDir().isPresent());
        assertEquals(Paths.get("/tmp/scratch/xyz"), decoded.getTmpDir().get());
    }
}
