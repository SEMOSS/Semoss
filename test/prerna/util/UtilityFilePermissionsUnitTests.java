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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link ClientProcessWrapper#startTCPServerNativePy} used to grant the
 * {@code PY_SERVER_USER} sudo target access to the insight working folder via
 * {@code File.setReadable/setWritable/setExecutable(true, false)}, which
 * grants read/write/execute to every local user ("others"), not just the
 * intended owner and group. It now calls
 * {@link Utility#setOwnerAndGroupPermissionsRecursively(java.io.File)}
 * instead (the same helper {@code AbstractVectorDatabaseEngine} already uses
 * for the identical cross-user access problem on the vector schema folder).
 * This test pins down that the helper actually narrows permissions to
 * owner+group only, with no access left for "others".
 */
class UtilityFilePermissionsUnitTests {

	@TempDir
	Path tempDir;

	@Test
	void setOwnerAndGroupPermissionsRecursivelyGrantsNoAccessToOthers() throws Exception {
		Assumptions.assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
				"POSIX file permissions are not supported on this platform");

		Path dir = tempDir.resolve("py-process-folder");
		Files.createDirectories(dir);
		Path nested = dir.resolve("nested.txt");
		Files.writeString(nested, "content");

		// Start from world-writable so the assertions below prove the helper
		// actually narrows the mode, rather than passing by coincidence of a
		// restrictive default umask.
		Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx"));
		Files.setPosixFilePermissions(nested, PosixFilePermissions.fromString("rwxrwxrwx"));

		Utility.setOwnerAndGroupPermissionsRecursively(dir.toFile());

		Set<PosixFilePermission> dirPerms = Files.getPosixFilePermissions(dir);
		assertEquals(
				EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE,
						PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ,
						PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE),
				dirPerms, "expected chmod 770 (owner+group rwx) with nothing granted to others");

		// the specific property the CodeQL "world writable" finding cares about
		assertFalse(dirPerms.contains(PosixFilePermission.OTHERS_READ), "others must not get read access");
		assertFalse(dirPerms.contains(PosixFilePermission.OTHERS_WRITE), "others must not get write access");
		assertFalse(dirPerms.contains(PosixFilePermission.OTHERS_EXECUTE), "others must not get execute access");

		Set<PosixFilePermission> filePerms = Files.getPosixFilePermissions(nested);
		assertFalse(filePerms.contains(PosixFilePermission.OTHERS_WRITE),
				"nested files must not be left world-writable either");
	}

}
