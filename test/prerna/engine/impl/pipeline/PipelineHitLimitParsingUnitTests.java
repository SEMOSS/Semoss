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
package prerna.engine.impl.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.engine.api.IEngine;
import prerna.reactor.interceptor.GenericGuardrailInputReactor;

class PipelineHitLimitParsingUnitTests {

	private static final String CHECK = "{\"reactorClass\":\"" + GenericGuardrailInputReactor.class.getName()
			+ "\",\"params\":{%s\"guardrailEngineId\":\"g\",\"inputMapping\":{\"prompt\":\"arg0\"}}}";

	@TempDir
	Path folder;

	@Test
	void limitsNameChecksByTheirIdOrByTheirMethodPhaseAndPlace() throws Exception {
		String pipelines = "{\"pipelines\":{\"askRoom\":{\"input\":[" + CHECK.formatted("\"checkId\":\"toxicity\",")
				+ "," + CHECK.formatted("") + "]}},"
				+ "\"hitLimits\":[{\"limitId\":\"lock\",\"checks\":[\"toxicity\"],\"hits\":3,\"scope\":\"USER\","
				+ "\"windowMinutes\":60,\"lockMinutes\":30},"
				// a check saved before checks had ids is named by its method, phase and place
				+ "{\"limitId\":\"close\",\"checks\":[\"askRoom.input.1\"],\"hits\":1,\"scope\":\"ROOM\"}]}";

		List<GuardrailHitLimit> limits = limitsOf(handler(pipelines));

		assertEquals(List.of(
				new GuardrailHitLimit("lock", Set.of("toxicity"), 3, GuardrailHitLimit.Scope.USER, 60, 30, null),
				new GuardrailHitLimit("close", Set.of("askRoom.input.1"), 1, GuardrailHitLimit.Scope.ROOM, 0, 0, null)),
				limits);
	}

	@Test
	void aLimitNamingNoCheckOfTheFileStopsTheEngineLoading() throws Exception {
		String pipelines = "{\"pipelines\":{\"askRoom\":{\"input\":[" + CHECK.formatted("") + "]}},"
				+ "\"hitLimits\":[{\"limitId\":\"close\",\"checks\":[\"gone\"],\"hits\":1,\"scope\":\"ROOM\"}]}";

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> handler(pipelines));
		assertTrue(refused.getMessage().contains("not a check in this pipeline file"), refused.getMessage());
	}

	private PipelineInvocationHandler handler(String pipelineJson) throws Exception {
		File file = this.folder.resolve("pipeline.json").toFile();
		Files.writeString(file.toPath(), pipelineJson, StandardCharsets.UTF_8);
		IEngine engine = (IEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
				new Class<?>[] { IEngine.class }, (proxy, method, arguments) -> switch (method.getName()) {
				case "getEngineId" -> "model-1";
				case "getEngineName" -> "Assistant";
				case "getCatalogType" -> IEngine.CATALOG_TYPE.MODEL;
				case "keepInputOutput" -> false;
				default -> null;
				});
		return new PipelineInvocationHandler(engine, file);
	}

	private static List<GuardrailHitLimit> limitsOf(PipelineInvocationHandler handler) throws Exception {
		Field field = PipelineInvocationHandler.class.getDeclaredField("hitLimits");
		field.setAccessible(true);
		return ((GuardrailHitLimits) field.get(handler)).limits();
	}
}
