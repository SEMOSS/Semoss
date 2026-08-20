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
package prerna.engine.impl.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

/**
 * Covers the guarantees the rclone process runner makes to every storage engine
 * built on it. Uses a shell rather than rclone itself so the exit code and the
 * output volume are both fully controlled.
 */
@EnabledOnOs({ OS.LINUX, OS.MAC })
public class AbstractRCloneStorageEngineUnitTests {

	@Test
	void testNonZeroExitFails() {
		IOException e = assertThrows(IOException.class,
				() -> AbstractRCloneStorageEngine.runAnyProcess("sh", "-c", "echo 'copy failed' >&2; exit 7"));
		assertTrue(e.getMessage().contains("exit code 7"), e.getMessage());
		assertTrue(e.getMessage().contains("copy failed"), e.getMessage());
	}

	@Test
	void testToleratedExitCodeReturnsEmpty() throws Exception {
		List<String> results = AbstractRCloneStorageEngine.runAnyProcess(Set.of(3, 4), "sh", "-c", "exit 3");
		assertTrue(results.isEmpty());
	}

	@Test
	void testToleratedExitCodeStillFailsOnOtherCodes() {
		assertThrows(IOException.class,
				() -> AbstractRCloneStorageEngine.runAnyProcess(Set.of(3, 4), "sh", "-c", "exit 5"));
	}

	/**
	 * Both pipes together hold far more than the OS pipe buffer. Draining them only
	 * after the process has been waited on deadlocks instead of returning.
	 */
	@Test
	// a separate thread so a regression aborts the build instead of hanging it
	@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
	void testLargeOutputOnBothPipesDoesNotBlock() throws Exception {
		List<String> results = AbstractRCloneStorageEngine.runAnyProcess("awk",
				"BEGIN { line = sprintf(\"%400s\", \"x\"); for (i = 0; i < 1000; i++) { print line; print line > \"/dev/stderr\" } }");
		assertEquals(1000, results.size());
	}

	/**
	 * A config create carries the storage credentials in its arguments, so neither
	 * the arguments nor the process error output may reach the caller.
	 */
	@Test
	void testConfigFailureDoesNotLeakCredentials(@TempDir Path tempDir) throws Exception {
		Path script = tempDir.resolve("fake-rclone.sh");
		Files.writeString(script, "#!/bin/sh\necho \"secret_access_key = $7\" >&2\nexit 1\n");
		assertTrue(script.toFile().setExecutable(true));

		IOException e = assertThrows(IOException.class,
				() -> AbstractRCloneStorageEngine.runAnyProcess(script.toString(), "config", "create", "myremote",
						"s3", "access_key_id", "AKIAEXAMPLE", "secret_access_key", "s3cr3t-value"));
		assertTrue(e.getMessage().contains("rclone config create"), e.getMessage());
		assertFalse(e.getMessage().contains("s3cr3t-value"), e.getMessage());
		assertFalse(e.getMessage().contains("AKIAEXAMPLE"), e.getMessage());
	}

	/**
	 * Drives the real public API against a stub rclone whose copy fails, the way a
	 * denied or unreachable transfer does. Guards the wiring, not just the runner:
	 * transfers must go through the strict exit-code policy.
	 */
	@Test
	void testCopyToStorageFailsWhenRcloneCopyFails(@TempDir Path tempDir) throws Exception {
		Path stub = tempDir.resolve("rclone");
		Files.writeString(stub, "#!/bin/sh\n"
				+ "if [ \"$1\" = \"copy\" ]; then\n"
				+ "  echo 'Failed to copy: AccessDenied: Access Denied' >&2\n"
				+ "  exit 1\n"
				+ "fi\n"
				+ "exit 0\n");
		assertTrue(stub.toFile().setExecutable(true));

		Path localFile = tempDir.resolve("payload.txt");
		Files.writeString(localFile, "hello");
		Files.writeString(tempDir.resolve("myconf.conf"), "[myconf]\ntype = s3\n");

		S3StorageEngine engine = new S3StorageEngine();
		engine.RCLONE = stub.toString();
		engine.setRCloneConfigFolder(tempDir.toString());

		IOException e = assertThrows(IOException.class,
				() -> engine.copyToStorage(localFile.toString(), "somefolder", "myconf", null));
		assertTrue(e.getMessage().contains("rclone copy failed"), e.getMessage());
	}

}
