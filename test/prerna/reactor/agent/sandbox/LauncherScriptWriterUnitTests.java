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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

public class LauncherScriptWriterUnitTests {

    @Test
    void writesFileWithShebang(@TempDir Path tmp) throws Exception {
        Path script = LauncherScriptWriter.write(tmp);
        String body = Files.readString(script);
        assertTrue(body.startsWith("#!/bin/sh"));
        assertTrue(body.contains("SEMOSS_SANDBOX_BACKEND"));
        assertTrue(body.contains("SEMOSS_SANDBOX_TARGET_CLI"));
        assertTrue(body.contains("landlock"));
        assertTrue(body.contains("sandbox-exec"));
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void scriptIsExecutableByOwner(@TempDir Path tmp) throws Exception {
        Path script = LauncherScriptWriter.write(tmp);
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(script);
        assertTrue(perms.contains(PosixFilePermission.OWNER_EXECUTE));
        assertTrue(perms.contains(PosixFilePermission.OWNER_READ));
    }

    @Test
    void eachWriteProducesUniquePath(@TempDir Path tmp) throws Exception {
        Path a = LauncherScriptWriter.write(tmp);
        Path b = LauncherScriptWriter.write(tmp);
        assertNotEquals(a, b);
        assertEquals(Files.readString(a), Files.readString(b));
    }
}
