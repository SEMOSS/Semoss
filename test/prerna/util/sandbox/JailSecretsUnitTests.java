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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.util.openshell.OpenShellSecrets;

class JailSecretsUnitTests {

	private static final String PROJECT = "1234abcd-0000-4000-8000-000000000001";
	private static final String OTHER_PROJECT = "9876fedc-0000-4000-8000-000000000002";

	@TempDir
	Path folder;

	@Test
	void theJailSeesTheFolderUnderTheSemossHomeAndTheHostKeepsItInTheUsersChroot() {
		assertEquals(Path.of("/opt/semosshome/sandbox-secrets"), JailSecrets.jailFolder("/opt/semosshome/"));
		assertEquals(Path.of("/chroots/u1/opt/semosshome/sandbox-secrets"),
				JailSecrets.hostFolder("/chroots/u1", "/opt/semosshome"));
	}

	@Test
	void onlyDeclaredSecretsWithAValueAreHandedOn() {
		Properties props = new Properties();
		props.setProperty(OpenShellSecrets.SECRETS_PROP,
				"[{\"name\":\"DCTM_AUTH\",\"hosts\":[\"dctm.example.com\"]},"
						+ "{\"name\":\"NO_VALUE\",\"hosts\":[\"api.example.com\"]},"
						+ "{\"name\":\"no_hosts\"}]");
		props.setProperty(OpenShellSecrets.VALUE_PROP_PREFIX + "DCTM_AUTH", " Basic c3ZjOnB3 ");
		props.setProperty(OpenShellSecrets.VALUE_PROP_PREFIX + "UNDECLARED", "x");
		props.setProperty(OpenShellSecrets.VALUE_PROP_PREFIX + "NO_HOSTS", "y");

		assertEquals(Map.of("DCTM_AUTH", "Basic c3ZjOnB3"), JailSecrets.values(PROJECT, props));
	}

	@Test
	void anUnreadableDeclarationHandsOnNothing() {
		Properties props = new Properties();
		props.setProperty(OpenShellSecrets.SECRETS_PROP, "not json");
		props.setProperty(OpenShellSecrets.VALUE_PROP_PREFIX + "DCTM_AUTH", "v");

		assertTrue(JailSecrets.values(PROJECT, props).isEmpty());
		assertTrue(JailSecrets.values(PROJECT, null).isEmpty());
	}

	@Test
	void eachProjectsSecretsAreKeptUnderItsOwnKeyAndReplacedWhole() throws IOException {
		Path secrets = this.folder.resolve("sandbox-secrets");

		JailSecrets.write(secrets, PROJECT, Map.of("A", "1", "B", "2"));
		JailSecrets.write(secrets, OTHER_PROJECT, Map.of("A", "3"));
		JailSecrets.write(secrets, PROJECT, Map.of("B", "4"));

		assertEquals(Map.of(OpenShellSecrets.projectKey(PROJECT), Map.of("B", "4"),
				OpenShellSecrets.projectKey(OTHER_PROJECT), Map.of("A", "3")),
				JailSecrets.read(secrets.resolve(JailSecrets.FILE)));
	}

	@Test
	void aProjectWhoseSecretsAreGoneIsTakenOut() throws IOException {
		Path secrets = this.folder.resolve("sandbox-secrets");
		JailSecrets.write(secrets, PROJECT, Map.of("A", "1"));
		JailSecrets.write(secrets, OTHER_PROJECT, Map.of("A", "3"));

		JailSecrets.write(secrets, PROJECT, Map.of());

		assertEquals(Map.of(OpenShellSecrets.projectKey(OTHER_PROJECT), Map.of("A", "3")),
				JailSecrets.read(secrets.resolve(JailSecrets.FILE)));
	}

	@Test
	void aProjectWithNoSecretsMakesNoFolder() throws IOException {
		Path secrets = this.folder.resolve("sandbox-secrets");

		JailSecrets.write(secrets, PROJECT, Map.of());

		assertFalse(Files.exists(secrets));
	}

	@Test
	void onlyTheOwnerCanReadTheFolderAndTheFile() throws IOException {
		Path secrets = this.folder.resolve("sandbox-secrets");

		JailSecrets.write(secrets, PROJECT, Map.of("A", "1"));

		assertEquals("rwx------", PosixFilePermissions.toString(Files.getPosixFilePermissions(secrets)));
		assertEquals("rw-------",
				PosixFilePermissions.toString(Files.getPosixFilePermissions(secrets.resolve(JailSecrets.FILE))));
		try (var files = Files.list(secrets)) {
			assertEquals(1, files.count(), "no staged file is left behind");
		}
	}

	@Test
	void anUnreadableFileIsReplaced() throws IOException {
		Path secrets = Files.createDirectories(this.folder.resolve("sandbox-secrets"));
		Files.writeString(secrets.resolve(JailSecrets.FILE), "{broken", StandardCharsets.UTF_8);

		JailSecrets.write(secrets, PROJECT, Map.of("A", "1"));

		assertEquals(Map.of(OpenShellSecrets.projectKey(PROJECT), Map.of("A", "1")),
				JailSecrets.read(secrets.resolve(JailSecrets.FILE)));
	}
}
