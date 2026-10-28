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
package prerna.engine.impl.model.serving;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class ServingCatalogUnitTests {

	private static final Set<String> RUNTIMES = Set.of("nvidia-vllm");

	@Test
	void readsTheShippedCatalog() {
		ServingCatalog catalog = ServingCatalog.parse(Fixtures.text("serving-catalog.yaml"), "test", RUNTIMES);

		assertTrue(catalog.getProblems().isEmpty(), catalog.getProblems().toString());
		ServingCatalog.Entry olmo = catalog.entry("allenai/OLMo-2-0425-1B-Instruct");
		assertEquals("hf://allenai/OLMo-2-0425-1B-Instruct", olmo.storageUri());
		assertEquals("nvidia-vllm", olmo.runtime());
		assertEquals("vllm", olmo.modelFormat());
		assertEquals(List.of("--gpu-memory-utilization=0.2"), olmo.args());
		assertEquals("2", olmo.cpu());
		assertEquals("4Gi", olmo.memory());
		assertEquals(2979627008L, olmo.sizeBytes());
		assertEquals(4096, olmo.contextWindow());
		assertEquals(1024, olmo.maxTokens());
		assertEquals("dgx-admin", olmo.approvedBy());
	}

	@Test
	void leavesOutAndReportsInvalidEntries() {
		ServingCatalog catalog = ServingCatalog.parse("""
				models:
				  - id: good
				    storageUri: hf://org/good
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: unapproved
				    storageUri: hf://org/unapproved
				    licence: apache-2.0
				    origin: Example (US)
				  - id: local-file
				    storageUri: file:///models/x
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: amd64-runtime
				    storageUri: hf://org/x
				    runtime: kserve-huggingfaceserver
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: remote-code
				    storageUri: hf://org/x
				    args: ["--trust-remote-code"]
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: good
				    storageUri: hf://org/again
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: wordy-limits
				    storageUri: hf://org/x
				    contextWindow: 4k
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				""", "test", RUNTIMES);

		assertEquals(List.of("good"), catalog.getEntries().stream().map(ServingCatalog.Entry::id).toList());
		String problems = String.join("\n", catalog.getProblems());
		assertEquals(6, catalog.getProblems().size(), problems);
		assertTrue(problems.contains("entry unapproved has no approvedBy"), problems);
		assertTrue(problems.contains("entry local-file has storageUri file:///models/x"), problems);
		assertTrue(problems.contains("names runtime kserve-huggingfaceserver"), problems);
		assertTrue(problems.contains("--trust-remote-code is not an allowed vLLM argument"), problems);
		assertTrue(problems.contains("entry good is listed twice"), problems);
		assertTrue(problems.contains("entry wordy-limits has contextWindow that is not a number"), problems);
	}

	@Test
	void anEntryOnS3RunsAsTheMinioServiceAccount() {
		ServingCatalog catalog = ServingCatalog.parse("""
				models:
				  - id: olmo-minio
				    storageUri: s3://semoss-models/allenai/OLMo-2-0425-1B-Instruct/0123456789abcdef0123456789abcdef01234567
				    licence: apache-2.0
				    origin: Ai2 (US)
				    approvedBy: admin
				  - id: own-account
				    storageUri: s3://other/x
				    serviceAccount: reader-of-other
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: hub
				    storageUri: hf://org/x
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: bad-account
				    storageUri: s3://other/x
				    serviceAccount: Not_A_Name
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				""", "test", RUNTIMES);

		assertEquals("kserve-minio", catalog.entry("olmo-minio").serviceAccount());
		assertEquals("reader-of-other", catalog.entry("own-account").serviceAccount());
		assertNull(catalog.entry("hub").serviceAccount(), "the Hub needs no account");
		assertEquals(List.of("test: entry bad-account has serviceAccount Not_A_Name, which is not a valid name"),
				catalog.getProblems());
	}

	@Test
	void anEntryOnAVolumeNamesAnAllowedClaimAndAFolderOfIt() {
		String commit = "0123456789abcdef0123456789abcdef01234567";
		ServingCatalog catalog = ServingCatalog.parse("""
				models:
				  - id: cached
				    storageUri: pvc://semoss-model-cache/allenai/OLMo-2-0425-1B-Instruct/%1$s
				    licence: apache-2.0
				    origin: Ai2 (US)
				    approvedBy: admin
				  - id: database
				    storageUri: pvc://postgres-data/base
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: whole-volume
				    storageUri: pvc://semoss-model-cache
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: climbs-out
				    storageUri: pvc://semoss-model-cache/allenai/../../x
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: half-copied
				    storageUri: pvc://semoss-model-cache/.incoming/semoss-intake-abc12
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				  - id: trailing-slash
				    storageUri: pvc://semoss-model-cache/allenai/OLMo-2-0425-1B-Instruct/%1$s/
				    licence: mit
				    origin: Example (US)
				    approvedBy: admin
				""".formatted(commit), "test", RUNTIMES);

		ServingCatalog.Entry cached = catalog.entry("cached");
		assertEquals("pvc://semoss-model-cache/allenai/OLMo-2-0425-1B-Instruct/" + commit, cached.storageUri());
		assertNull(cached.serviceAccount(), "a volume needs no account");
		String problems = String.join("\n", catalog.getProblems());
		assertEquals(5, catalog.getProblems().size(), problems);
		assertTrue(problems.contains("entry database has storageUri pvc://postgres-data/base on volume postgres-data, "
				+ "which is not one of [semoss-model-cache] (KSERVE_WEIGHT_VOLUMES)"), problems);
		for (String refused : List.of("whole-volume", "climbs-out", "half-copied", "trailing-slash")) {
			assertTrue(problems.contains("entry " + refused + " has storageUri "), problems);
		}
		assertTrue(problems.contains("which must name the folder of the weights on the volume, such as "
				+ "pvc://semoss-model-cache/<org>/<repo>/<commit>"), problems);

		ServingCatalog other = ServingCatalog.parse("""
				models:
				  - id: elsewhere
				    storageUri: pvc://team-weights/granite/main
				    licence: apache-2.0
				    origin: IBM (US)
				    approvedBy: admin
				  - id: default-cache
				    storageUri: pvc://semoss-model-cache/a/b/c
				    licence: apache-2.0
				    origin: IBM (US)
				    approvedBy: admin
				""", "test", RUNTIMES, Set.of("team-weights"));
		assertEquals(List.of("elsewhere"), other.getEntries().stream().map(ServingCatalog.Entry::id).toList());
		assertTrue(other.getProblems().get(0).contains("on volume semoss-model-cache, which is not one of [team-weights]"),
				other.getProblems().toString());
	}

	@Test
	void unknownEntriesCannotBeDefined() {
		ServingCatalog catalog = ServingCatalog.parse(Fixtures.text("serving-catalog.yaml"), "ConfigMap semoss/x",
				RUNTIMES);
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> catalog.entry("example/unapproved-model"));
		assertTrue(error.getMessage().contains("is not in the serving catalog (ConfigMap semoss/x)"), error.getMessage());
	}

	@Test
	void brokenDocumentsAreAProblemNotACrash() {
		assertTrue(ServingCatalog.parse("models: [", "test", RUNTIMES).getProblems().get(0).contains("not valid YAML"));
		assertTrue(ServingCatalog.parse("other: 1", "test", RUNTIMES).getProblems().get(0).contains("no models list"));
		assertTrue(ServingCatalog.parse(null, "test", RUNTIMES).getEntries().isEmpty());
	}
}
