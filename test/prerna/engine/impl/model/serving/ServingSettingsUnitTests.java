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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.serving.ServedModelEndpoints.Mode;
import prerna.engine.impl.model.serving.ServingSettings.Auth;

class ServingSettingsUnitTests {

	@Test
	void theHostPortRangeIsTwoPorts() {
		assertEquals(new ServingSettings.PortRange(8000, 8099), ServingSettings.hostPortRange(null));
		assertEquals(new ServingSettings.PortRange(9000, 9010), ServingSettings.hostPortRange(" 9000 - 9010 "));
		for (String bad : new String[] { "80-90", "9010-9000", "8000", "8000-70000", "eight-nine" }) {
			IllegalStateException error = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
					() -> ServingSettings.hostPortRange(bad), bad);
			org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().startsWith("KSERVE_HOST_PORT_RANGE must be"),
					error.getMessage());
		}
	}

	@Test
	void theMemoryBudgetIsAFractionOfTheUnifiedMemory() {
		assertEquals(0.7, ServingSettings.memoryBudget(null));
		assertEquals(0.5, ServingSettings.memoryBudget("0.5"));
		assertEquals(1.0, ServingSettings.memoryBudget("1"));
		for (String bad : new String[] { "0", "1.2", "-0.1", "most" }) {
			IllegalStateException error = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
					() -> ServingSettings.memoryBudget(bad), bad);
			org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().startsWith("KSERVE_MEMORY_BUDGET must be a fraction"),
					error.getMessage());
		}
	}

	@Test
	void weightVolumesDefaultToTheModelCache() {
		assertEquals(java.util.Set.of("semoss-model-cache"), ServingSettings.weightVolumes(null));
		assertEquals(java.util.Set.of("semoss-model-cache"), ServingSettings.weightVolumes(" , "));
		assertEquals(java.util.Set.of("team-weights", "semoss-model-cache"),
				ServingSettings.weightVolumes(" team-weights , semoss-model-cache "));
		IllegalStateException error = assertThrows(IllegalStateException.class,
				() -> ServingSettings.weightVolumes("semoss-model-cache, Postgres_Data"));
		assertEquals("KSERVE_WEIGHT_VOLUMES lists Postgres_Data, which is not a PersistentVolumeClaim name",
				error.getMessage());
	}

	@Test
	void credentialsFollowWhereSemossRuns() {
		assertEquals(Auth.IN_CLUSTER, ServingSettings.auth(null, false, true));
		assertEquals(Auth.KUBECONFIG, ServingSettings.auth(null, true, true), "a named kubeconfig wins in a pod");
		assertEquals(Auth.KUBECONFIG, ServingSettings.auth("auto", false, false));
		assertEquals(Auth.IN_CLUSTER, ServingSettings.auth("InCluster", false, false));
		assertEquals(Auth.KUBECONFIG, ServingSettings.auth("kubeconfig", false, true));
		assertThrows(IllegalStateException.class, () -> ServingSettings.auth("token", false, false));
	}

	@Test
	void networkModeFollowsWhereSemossRuns() {
		assertEquals(Mode.CLUSTER, ServingSettings.networkMode(null, true));
		assertEquals(Mode.HOST, ServingSettings.networkMode("auto", false));
		assertEquals(Mode.HOST, ServingSettings.networkMode("Host", true),
				"SEMOSS in a pod can still be told to use host endpoints");
		assertEquals(Mode.CLUSTER, ServingSettings.networkMode("cluster", false));
		assertThrows(IllegalStateException.class, () -> ServingSettings.networkMode("lan", false));
	}

	@Test
	void prometheusAndGrafanaAreAutoOffOrAUrl() {
		String key = ServingSettings.KSERVE_PROMETHEUS_URL;
		assertEquals(null, ServingSettings.urlOrOff(key, null, "http://10.43.0.10:9090"));
		assertEquals(null, ServingSettings.urlOrOff(key, "Auto", "http://10.43.0.10:9090"));
		assertEquals(ServingSettings.OFF, ServingSettings.urlOrOff(key, "OFF", "http://10.43.0.10:9090"));
		assertEquals("http://10.43.12.7:9090", ServingSettings.urlOrOff(key, "http://10.43.12.7:9090/", "x"));
		assertEquals("https://metrics.example.org/prometheus",
				ServingSettings.urlOrOff(key, "https://metrics.example.org/prometheus", "x"));
		for (String bad : new String[] { "10.43.12.7:9090", "ftp://10.43.12.7", "http://", "http://host:9090/?x=1",
				"no url" }) {
			IllegalStateException error = assertThrows(IllegalStateException.class,
					() -> ServingSettings.urlOrOff(key, bad, "http://10.43.0.10:9090"), bad);
			assertEquals("KSERVE_PROMETHEUS_URL must be auto, off or an http(s) URL such as http://10.43.0.10:9090; it is "
					+ bad, error.getMessage());
		}
	}
}
