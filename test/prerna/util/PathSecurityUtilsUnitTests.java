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
package prerna.util;

import static org.junit.jupiter.api.Assertions.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PathSecurityUtilsUnitTests {
    @TempDir
    Path directory;

    @Test
    void preservesIdentifiersAndDisplayNames() {
        for (String value : new String[] {"abc-123", "platform__room", "Model Name", "caf\u00e9"}) {
            assertEquals(value, PathSecurityUtils.requireSinglePathSegment(value, "ID"));
        }
    }

    @Test
    void rejectsTraversalAndCrossPlatformSeparators() {
        for (String value : new String[] {"", " ", ".", "..", "../other", "a/b", "a\\b",
                "C:other", "a\n", "\uff0e\uff0e", "a\uff0fb", "a\uff3cb"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> PathSecurityUtils.requireSinglePathSegment(value, "ID"), value);
        }
        assertThrows(IllegalArgumentException.class,
                () -> PathSecurityUtils.requireSinglePathSegment(null, "ID"));
    }

    @Test
    void identifierValidationDoesNotDependOnFilesystem() throws Exception {
        Files.createSymbolicLink(directory.resolve("model"), directory.resolve("elsewhere"));
        assertEquals("model", PathSecurityUtils.requireSinglePathSegment("model", "ID"));
    }

    @Test
    void directChildAllowsNewFilesButRejectsNestedAndSiblingPaths() throws Exception {
        File root = directory.resolve("assets").toFile();
        assertEquals(new File(root, "pipeline.json").getCanonicalFile(),
                PathSecurityUtils.requireDirectChild(root, new File(root, "pipeline.json")));
        assertThrows(IllegalArgumentException.class,
                () -> PathSecurityUtils.requireDirectChild(root, new File(root, "nested/file")));
        assertThrows(IllegalArgumentException.class,
                () -> PathSecurityUtils.requireDirectChild(root, new File(root, "../other/file")));
        assertThrows(IllegalArgumentException.class, () -> PathSecurityUtils.requireDirectChild(root, root));
    }

    @Test
    void descendantAllowsNestedPathsButRejectsRootAndPrefixSibling() throws Exception {
        File root = directory.resolve("assets").toFile();
        File nested = new File(root, "nested/file");
        assertEquals(nested.getCanonicalFile(), PathSecurityUtils.requireDescendant(root, nested));
        assertThrows(IllegalArgumentException.class, () -> PathSecurityUtils.requireDescendant(root, root));
        assertThrows(IllegalArgumentException.class,
                () -> PathSecurityUtils.requireDescendant(root, directory.resolve("assets-other/file").toFile()));
    }

    @Test
    void rejectsExistingSymlinksOutsideRoot() throws Exception {
        Path root = Files.createDirectory(directory.resolve("assets"));
        Path outside = Files.createDirectory(directory.resolve("outside"));
        Path link = Files.createSymbolicLink(root.resolve("link"), outside);
        assertThrows(IllegalArgumentException.class,
                () -> PathSecurityUtils.requireDirectChild(root.toFile(), link.toFile()));
        assertThrows(IllegalArgumentException.class,
                () -> PathSecurityUtils.requireDescendant(root.toFile(), link.resolve("file").toFile()));
    }
}
