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

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.common.Factory;
import net.schmizz.sshj.common.SecurityUtils;

class SFTPStorageEngineSshConfigUnitTests {

	@TempDir
	Path temporaryDirectory;

	// Provider registration and approved-only mode are JVM state. Separate JVMs
	// also check that the standard classpath can retain ordinary BC dependencies.
	@ParameterizedTest
	@ValueSource(strings = { "standard", "standard-unset", "fips", "missing", "general", "reordered" })
	void verifiesRuntimePolicyInFreshJvm(String scenario) throws Exception {
		String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		if (!scenario.startsWith("standard")) {
			classpath = Arrays.stream(classpath.split(java.io.File.pathSeparator))
					.filter(entry -> !Path.of(entry).getFileName().toString().matches("bc(prov|pkix|util)-jdk.*"))
					.collect(Collectors.joining(java.io.File.pathSeparator));
		}
		List<String> command = new ArrayList<>(
				List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp", classpath));
		if (!scenario.equals("standard-unset")) {
			command.add("-Dorg.bouncycastle.fips.approved_only="
					+ !(scenario.equals("standard") || scenario.equals("general")));
		}
		if (List.of("fips", "general", "reordered").contains(scenario)) {
			String bc = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider";
			String sun = "sun.security.provider.Sun";
			Path security = temporaryDirectory.resolve("java.security");
			// Preserve the JDK's entropy/strong-random settings when replacing providers.
			Properties properties = new Properties();
			try (var reader = Files
					.newBufferedReader(Path.of(System.getProperty("java.home"), "conf", "security", "java.security"))) {
				properties.load(reader);
			}
			properties.keySet().removeIf(key -> key.toString().startsWith("security.provider."));
			properties.setProperty("security.provider.1", scenario.equals("reordered") ? sun : bc);
			properties.setProperty("security.provider.2", scenario.equals("reordered") ? bc : sun);
			try (var writer = Files.newBufferedWriter(security)) {
				properties.store(writer, "SSH configuration test providers");
			}
			command.add("-Djava.security.properties==" + security);
		}
		command.add(SFTPStorageEngineSshConfigUnitTests.class.getName());
		command.add(scenario);
		Path output = temporaryDirectory.resolve("output.log");
		ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile());
		builder.environment().remove("JAVA_TOOL_OPTIONS");
		builder.environment().remove("JDK_JAVA_OPTIONS");
		builder.environment().put("SEMOSS_FIPS", Boolean.toString(scenario.equals("general")));
		Process child = builder.start();
		try {
			assertTrue(child.waitFor(60, TimeUnit.SECONDS), "SSH configuration probe timed out");
			assertEquals(0, child.exitValue(), () -> readOutput(output));
		} finally {
			child.destroyForcibly();
		}
	}

	private static String readOutput(Path path) {
		try {
			return Files.readString(path);
		} catch (java.io.IOException e) {
			return e.toString();
		}
	}

	private static List<String> names(List<? extends Factory.Named<?>> factories) {
		return factories.stream().map(Factory.Named::getName).toList();
	}

	public static void main(String[] args) throws Exception {
		String scenario = args[0];
		if (List.of("missing", "general", "reordered").contains(scenario)) {
			IllegalStateException error = assertThrows(IllegalStateException.class, SFTPStorageEngine::createSshConfig);
			assertTrue(error.getMessage().contains("approved-only"));
			return;
		}
		DefaultConfig actual = SFTPStorageEngine.createSshConfig();
		if (scenario.startsWith("standard")) {
			DefaultConfig expected = new DefaultConfig();
			assertEquals(names(expected.getKeyExchangeFactories()), names(actual.getKeyExchangeFactories()));
			assertEquals(names(expected.getKeyAlgorithms()), names(actual.getKeyAlgorithms()));
			assertEquals(names(expected.getCipherFactories()), names(actual.getCipherFactories()));
			assertEquals(names(expected.getMACFactories()), names(actual.getMACFactories()));
			return;
		}
		assertEquals("BCFIPS", Security.getProviders()[0].getName());
		assertTrue(CryptoServicesRegistrar.isInApprovedOnlyMode());
		assertFalse(names(actual.getKeyExchangeFactories()).stream()
				.anyMatch(name -> name.contains("25519") || name.endsWith("sha1") || name.contains("group-exchange")));
		assertTrue(names(actual.getKeyExchangeFactories()).contains("ext-info-c"));
		assertFalse(names(actual.getKeyAlgorithms()).stream().anyMatch(name -> name.equals("ssh-rsa")
				|| name.equals("ssh-dss") || name.contains("-cert-") || name.startsWith("sk-")));
		assertTrue(names(actual.getCipherFactories()).stream().allMatch(name -> name.matches("aes(128|192|256)-ctr")));
		assertTrue(names(actual.getMACFactories()).stream().allMatch(name -> name.startsWith("hmac-sha2-")));
		assertEquals("BCFIPS", SecurityUtils.getCipher("AES/CTR/NoPadding").getProvider().getName());
		for (String signature : List.of("SHA256withRSA", "SHA512withRSA", "SHA256withECDSA", "SHA384withECDSA",
				"SHA512withECDSA", "Ed25519")) {
			assertEquals("BCFIPS", SecurityUtils.getSignature(signature).getProvider().getName());
		}
	}
}
