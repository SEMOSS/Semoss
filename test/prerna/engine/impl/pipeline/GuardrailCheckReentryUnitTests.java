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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.engine.api.IEngine;
import prerna.reactor.IReactor;
import prerna.reactor.interceptor.GenericGuardrailInputReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class GuardrailCheckReentryUnitTests {

	private static final String DELETE_PIPELINE = "{\"pipelines\":{\"delete\":{\"input\":[{\"reactorClass\":\""
			+ GenericGuardrailInputReactor.class.getName()
			+ "\",\"params\":{\"guardrailEngineId\":\"g\",\"inputMapping\":{\"prompt\":\"arg0\"}}}]}}}";

	@TempDir
	Path folder;

	@Test
	void aCallFromInsideTheEnginesOwnCheckReachesTheEngineWithoutRunningTheCheck() throws Exception {
		AtomicInteger deletes = new AtomicInteger();
		PipelineInvocationHandler handler = handler(deletes);
		Method delete = IEngine.class.getMethod("delete");

		runCheck(handler, check(() -> {
			try {
				handler.invoke(null, delete, null);
			} catch (Throwable e) {
				throw new IllegalStateException(e);
			}
		}));

		assertEquals(1, deletes.get());
	}

	@Test
	void theEngineIsNoLongerMarkedOnceItsCheckFinishes() throws Exception {
		PipelineInvocationHandler handler = handler(new AtomicInteger());

		runCheck(handler, check(() -> {
		}));

		Field running = PipelineInvocationHandler.class.getDeclaredField("ENGINES_RUNNING_CHECKS");
		running.setAccessible(true);
		assertNull(((ThreadLocal<?>) running.get(null)).get());
	}

	private PipelineInvocationHandler handler(AtomicInteger deletes) throws Exception {
		File file = this.folder.resolve("pipeline.json").toFile();
		Files.writeString(file.toPath(), DELETE_PIPELINE, StandardCharsets.UTF_8);
		IEngine engine = (IEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
				new Class<?>[] { IEngine.class }, (proxy, method, arguments) -> switch (method.getName()) {
				case "getEngineId" -> "model-1";
				case "getEngineName" -> "Assistant";
				case "getCatalogType" -> IEngine.CATALOG_TYPE.MODEL;
				case "keepInputOutput" -> false;
				case "delete" -> {
					deletes.incrementAndGet();
					yield null;
				}
				default -> null;
				});
		return new PipelineInvocationHandler(engine, file);
	}

	private static IReactor check(Runnable body) {
		return (IReactor) Proxy.newProxyInstance(IReactor.class.getClassLoader(), new Class<?>[] { IReactor.class },
				(proxy, method, arguments) -> {
					if ("execute".equals(method.getName())) {
						body.run();
						return new NounMetadata(true, PixelDataType.BOOLEAN);
					}
					return null;
				});
	}

	private static void runCheck(PipelineInvocationHandler handler, IReactor check) throws Exception {
		Method runCheck = PipelineInvocationHandler.class.getDeclaredMethod("runCheck", IReactor.class);
		runCheck.setAccessible(true);
		runCheck.invoke(handler, check);
	}
}
