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
package prerna.util.sandbox;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class NamespaceSandboxUnitTests {

	/**
	 * A worker that never became ready is owned by nobody once spawnWorker throws, so the failure
	 * path has to stop the launcher and its children and remove every folder it was given.
	 */
	@Test
	void discardStopsTheLauncherAndRemovesItsFolders() throws Exception {
		Path root = Files.createTempDirectory("ns-discard");
		Path server = Files.createDirectories(root.resolve("a1"));
		Path io = Files.createDirectories(root.resolve("io"));
		Path jail = Files.createDirectories(root.resolve("io_j"));
		Path control = Files.createDirectories(root.resolve("io_c"));
		Files.writeString(io.resolve("console.txt"), "log");

		// a launcher stand-in that starts a child, as the real launcher does
		Process launcher = new ProcessBuilder("/bin/sh", "-c", "sleep 300 & wait").start();
		long deadline = System.currentTimeMillis() + 5_000;
		while (launcher.descendants().count() == 0 && System.currentTimeMillis() < deadline) {
			Thread.sleep(50);
		}
		assertTrue(launcher.isAlive());
		assertTrue(launcher.descendants().count() > 0, "the stand-in launcher never started its child");
		java.util.List<ProcessHandle> children = launcher.descendants().toList();

		WorkerHandle handle = new WorkerHandle(launcher, null, null, 0, server.toString(), control + "/c.sock",
				io.toString(), jail.toString(), control.toString(), null, false, false);
		NamespaceSandbox.discard(handle);

		assertTrue(launcher.waitFor(5, java.util.concurrent.TimeUnit.SECONDS), "the launcher is still running");
		for (ProcessHandle child : children) {
			long until = System.currentTimeMillis() + 5_000;
			while (child.isAlive() && System.currentTimeMillis() < until) {
				Thread.sleep(50);
			}
			assertFalse(child.isAlive(), "a child of the launcher is still running");
		}
		for (Path folder : new Path[] { server, io, jail, control }) {
			assertFalse(Files.exists(folder), folder + " was left behind");
		}
		Files.deleteIfExists(root);
	}

	@Test
	void discardToleratesAHandleWithNothingToStop() {
		NamespaceSandbox.discard(new WorkerHandle(null, null, null, 0, null, null, null, null, null, null, false, false));
	}
}
