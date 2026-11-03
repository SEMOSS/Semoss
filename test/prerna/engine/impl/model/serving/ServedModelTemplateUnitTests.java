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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import prerna.util.k8s.KubeJson;

class ServedModelTemplateUnitTests {

	private static ServingCatalog.Entry olmo() {
		return ServingCatalog.parse(Fixtures.text("serving-catalog.yaml"), "test", Set.of("nvidia-vllm"))
				.entry("allenai/OLMo-2-0425-1B-Instruct");
	}

	private static JsonObject action() {
		JsonObject action = new JsonObject();
		action.addProperty("action", "define");
		return action;
	}

	@Test
	void rendersAStoppedManagedInferenceService() {
		// pixel numbers arrive as doubles
		ServedModelTemplate.Overrides overrides = ServedModelTemplate.Overrides
				.from(Map.of("gpuMemoryUtilization", 0.25, "maxModelLen", 2048.0, "cpu", 4.0, "memory", "8Gi"));

		JsonObject isvc = ServedModelTemplate.inferenceService("olmo-2-1b", "semoss", olmo(), overrides, false,
				action());

		assertEquals("olmo-2-1b", KubeJson.str(isvc, "metadata", "name"));
		assertEquals("semoss", KubeJson.str(isvc, "metadata", "labels", "app.kubernetes.io/managed-by"));
		assertEquals("true", KubeJson.str(isvc, "metadata", "annotations", "serving.kserve.io/stop"));
		assertEquals("Standard", KubeJson.str(isvc, "metadata", "annotations", "serving.kserve.io/deploymentMode"));
		assertEquals("allenai/OLMo-2-0425-1B-Instruct",
				KubeJson.str(isvc, "metadata", "annotations", "semoss.org/catalog-entry"));
		assertEquals("Recreate", KubeJson.str(isvc, "spec", "predictor", "deploymentStrategy", "type"));
		assertEquals(1, KubeJson.integer(isvc, "spec", "predictor", "maxReplicas"));
		JsonObject model = KubeJson.obj(isvc, "spec", "predictor", "model");
		assertEquals("vllm", KubeJson.str(model, "modelFormat", "name"));
		assertEquals("nvidia-vllm", KubeJson.str(model, "runtime"));
		assertEquals("hf://allenai/OLMo-2-0425-1B-Instruct", KubeJson.str(model, "storageUri"));
		assertEquals(List.of("--gpu-memory-utilization=0.2", "--gpu-memory-utilization=0.25", "--max-model-len=2048"),
				KubeJson.strings(model, "args"), "the override comes last, and vLLM takes the last value");
		assertEquals("4", KubeJson.str(model, "resources", "requests", "cpu"));
		assertEquals("8Gi", KubeJson.str(model, "resources", "requests", "memory"));
		assertEquals("1", KubeJson.str(model, "resources", "limits", "nvidia.com/gpu"));
		assertNull(KubeJson.str(model, "resources", "limits", "memory"), "no memory limit on unified memory");
		assertEquals(0.25, ServedModelStateResolver.lastFlagValue(KubeJson.strings(model, "args"),
				"--gpu-memory-utilization"));
	}

	@Test
	void ownedServiceIsWhatTheEndpointsPick() {
		JsonObject isvc = ServedModelTemplate.inferenceService("olmo-2-1b", "semoss", olmo(),
				ServedModelTemplate.Overrides.NONE, true, action());
		assertEquals("false", KubeJson.str(isvc, "metadata", "annotations", "serving.kserve.io/stop"));
		JsonObject service = ServedModelTemplate.endpointService("olmo-2-1b", "semoss", "uid-1");

		assertEquals("olmo-2-1b-endpoint", KubeJson.str(service, "metadata", "name"));
		JsonObject owner = KubeJson.objects(service, "metadata", "ownerReferences").get(0);
		assertEquals("InferenceService", KubeJson.str(owner, "kind"));
		assertEquals("uid-1", KubeJson.str(owner, "uid"));
		assertFalse(KubeJson.isTrue(owner, "blockOwnerDeletion"));
		assertEquals("ClusterIP", KubeJson.str(service, "spec", "type"));

		assertEquals("http://olmo-2-1b-endpoint.semoss.svc.cluster.local/v1",
				new ServedModelEndpoints(ServedModelEndpoints.Mode.CLUSTER, "localhost").resolve(isvc, List.of(service))
						.url());
		service.getAsJsonObject("spec").addProperty("clusterIP", "10.43.0.90");
		assertEquals("http://10.43.0.90/v1",
				new ServedModelEndpoints(ServedModelEndpoints.Mode.HOST, "localhost").resolve(isvc, List.of(service))
						.url());
	}

	@Test
	void allowsOnlyCheckedVllmArguments() {
		assertEquals(List.of("--max-num-seqs=64", "--dtype=bfloat16", "--enable-prefix-caching",
				"--tool-call-parser=hermes"), ServedModelTemplate.validateArgs(List.of(" --max-num-seqs=64",
						"--dtype=bfloat16", "--enable-prefix-caching", "--tool-call-parser=hermes"), "test"));
		for (String refused : List.of("--served-model-name=other", "--trust-remote-code", "--host=0.0.0.0",
				"--port=9000", "--gpu-memory-utilization=0.95", "--max-model-len=abc", "--dtype=int4",
				"--quantization=a b", "--kv-cache-dtype=$(id)", "--enable-prefix-caching=1", "--max-num-seqs")) {
			assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.validateArgs(List.of(refused), "test"),
					refused);
		}
	}

	@Test
	void namesAreDnsLabelsShortEnoughForTheirSuffixes() {
		ServedModelTemplate.validateName("olmo-2-1b");
		ServedModelTemplate.validateName("a".repeat(ServedModelTemplate.MAX_NAME_LENGTH));
		for (String refused : List.of("Olmo", "-olmo", "olmo-", "granite-3.3", "1b-model", "a".repeat(46), "")) {
			assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.validateName(refused), refused);
		}
	}

	@Test
	void overridesAreCheckedToo() {
		assertEquals("0.5", ServedModelTemplate.Overrides.from(Map.of("cpu", 0.5)).cpu());
		assertTrue(assertThrows(IllegalArgumentException.class,
				() -> ServedModelTemplate.Overrides.from(Map.of("replicas", 2))).getMessage().contains("Unknown override"));
		assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.Overrides.from(Map.of("args", "--x")));
		assertThrows(IllegalArgumentException.class,
				() -> ServedModelTemplate.Overrides.from(Map.of("memory", "lots")));
		assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.args(olmo(),
				ServedModelTemplate.Overrides.from(Map.of("gpuMemoryUtilization", 0.99))));
	}

	@Test
	void aPublishedModelGetsTheLowestFreePort() {
		List<JsonObject> services = Fixtures.items("services-running.json");
		ServingSettings.PortRange range = new ServingSettings.PortRange(8000, 8099);

		assertEquals(8001, ServedModelTemplate.hostPort(services, range, null), "8000 is the sample's, 8080 SEMOSS's");
		assertEquals(8005, ServedModelTemplate.hostPort(services, range, 8005));
		assertEquals("Port 8000 is already taken by a LoadBalancer Service",
				assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.hostPort(services, range, 8000))
						.getMessage());
		assertEquals("Port 8080 is already taken (reserved)",
				assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.hostPort(services, range, 8080))
						.getMessage());
		assertEquals("publishPort 9000 is outside KSERVE_HOST_PORT_RANGE 8000-8099",
				assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.hostPort(services, range, 9000))
						.getMessage());
		assertTrue(assertThrows(IllegalArgumentException.class, () -> ServedModelTemplate.hostPort(services,
				new ServingSettings.PortRange(8000, 8000), null)).getMessage()
				.startsWith("Every port of KSERVE_HOST_PORT_RANGE 8000-8000 is taken"));
		assertEquals(9400 + 1, ServedModelTemplate.hostPort(List.of(), new ServingSettings.PortRange(9400, 9401), null),
				"the DCGM exporter's port is reserved");
	}

	@Test
	void aPublishedServiceIsALoadBalancerOnTheHostPort() {
		JsonObject published = ServedModelTemplate.endpointService("olmo-2-1b", "semoss", "uid-1", 8001);
		JsonObject internal = ServedModelTemplate.endpointService("olmo-2-1b", "semoss", "uid-1");

		assertEquals("LoadBalancer", KubeJson.str(published, "spec", "type"));
		JsonObject port = KubeJson.objects(published, "spec", "ports").get(0);
		assertEquals(8001, KubeJson.integer(port, "port"));
		assertEquals(8080, KubeJson.integer(port, "targetPort"));
		assertEquals("ClusterIP", KubeJson.str(internal, "spec", "type"));
		assertEquals(80, KubeJson.integer(KubeJson.objects(internal, "spec", "ports").get(0), "port"));
	}

	@Test
	void aModelOnS3RunsAsTheServiceAccountThatReadsIt() {
		ServingCatalog.Entry hub = olmo();
		ServingCatalog.Entry minio = ServingCatalog.parse(Fixtures.text("serving-catalog.yaml").replace(
				"storageUri: hf://allenai/OLMo-2-0425-1B-Instruct",
				"storageUri: s3://semoss-models/allenai/OLMo-2-0425-1B-Instruct/0123456789abcdef0123456789abcdef01234567"),
				"test", Set.of("nvidia-vllm")).entry("allenai/OLMo-2-0425-1B-Instruct");

		JsonObject fromMinio = ServedModelTemplate.inferenceService("olmo-2-1b", "semoss", minio,
				ServedModelTemplate.Overrides.from(null), false, action());
		JsonObject fromHub = ServedModelTemplate.inferenceService("olmo-2-1b", "semoss", hub,
				ServedModelTemplate.Overrides.from(null), false, action());

		assertEquals("kserve-minio", KubeJson.str(fromMinio, "spec", "predictor", "serviceAccountName"));
		assertEquals("s3://semoss-models/allenai/OLMo-2-0425-1B-Instruct/0123456789abcdef0123456789abcdef01234567",
				KubeJson.str(fromMinio, "spec", "predictor", "model", "storageUri"));
		assertNull(KubeJson.str(fromHub, "spec", "predictor", "serviceAccountName"), "the Hub needs no account");
		assertEquals("2", KubeJson.str(fromMinio, "metadata", "annotations", "semoss.org/template-version"));
	}
}
