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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.logging.IgnoreEngineLogging;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.IReactor;
import prerna.reactor.interceptor.GenericGuardrailInputReactor;
import prerna.reactor.interceptor.IInputReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

class SqlGuardrailPipelineContextUnitTests {

	private String storedInsightId;

	@AfterEach
	void clearThreadContext() {
		ThreadStore.remove();
		if (this.storedInsightId != null) {
			InsightStore.getInstance().remove(this.storedInsightId);
		}
	}

	@Test
	void actualTargetEngineIsBoundDirectlyAndTheOverloadCannotBeEngineLogged() throws Exception {
		PipelineInvocationHandler handler = handler();
		GenericGuardrailInputReactor reactor = new GenericGuardrailInputReactor();

		invoke(handler, "bindTargetEngine", new Class<?>[] { IReactor.class }, reactor);

		java.lang.reflect.Field targetEngineField = GenericGuardrailInputReactor.class.getDeclaredField("targetEngine");
		targetEngineField.setAccessible(true);
		IEngine capturedEngine = (IEngine) targetEngineField.get(reactor);
		assertEquals("database-id", capturedEngine.getEngineId());
		Method executeWithEngine = IGuardrailReactorFunctionEngine.class.getMethod("execute", NounStore.class,
				GenRowStruct.class, IEngine.class);
		assertTrue(executeWithEngine.isAnnotationPresent(IgnoreEngineLogging.class));
		assertTrue(Modifier
				.isTransient(GenericGuardrailInputReactor.class.getDeclaredField("targetEngine").getModifiers()));
	}

	@Test
	void everyInvocationGetsFreshMutableReactorState() throws Exception {
		PipelineInvocationHandler handler = handler();
		IInputReactor prototype = new GenericGuardrailInputReactor();

		IInputReactor first = (IInputReactor) invoke(handler, "newInvocationReactor",
				new Class<?>[] { IReactor.class, Class.class }, prototype, IInputReactor.class);
		IInputReactor second = (IInputReactor) invoke(handler, "newInvocationReactor",
				new Class<?>[] { IReactor.class, Class.class }, prototype, IInputReactor.class);

		assertNotSame(prototype, first);
		assertNotSame(first, second);
	}

	@Test
	void callerInsightComesFromThreadContextAndIsExplicitlyCleared() throws Exception {
		PipelineInvocationHandler handler = handler();
		Insight insight = new Insight();
		this.storedInsightId = "insight-id";
		InsightStore.getInstance().put(this.storedInsightId, insight);
		ThreadStore.setInsightId(this.storedInsightId);
		AtomicReference<Insight> capturedInsight = new AtomicReference<>();
		IReactor contextualReactor = capturingReactor(capturedInsight);

		invoke(handler, "setContextInsight", new Class<?>[] { IReactor.class, Object[].class }, contextualReactor,
				new Object[] { "SELECT 1" });
		assertEquals(insight, capturedInsight.get());

		ThreadStore.remove();
		capturedInsight.set(insight);
		IReactor contextFreeReactor = capturingReactor(capturedInsight);
		invoke(handler, "setContextInsight", new Class<?>[] { IReactor.class, Object[].class }, contextFreeReactor,
				new Object[] { "SELECT 1" });
		assertEquals(null, capturedInsight.get());
	}

	private static PipelineInvocationHandler handler() {
		IEngine engine = (IEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
				new Class<?>[] { IEngine.class }, (proxy, method, arguments) -> {
					switch (method.getName()) {
					case "getEngineId":
						return "database-id";
					case "getEngineName":
						return "Orders";
					case "getCatalogType":
						return IEngine.CATALOG_TYPE.DATABASE;
					case "getCatalogSubType":
						return "RDBMS";
					default:
						return defaultValue(method.getReturnType());
					}
				});
		return new PipelineInvocationHandler(engine, null);
	}

	private static IReactor capturingReactor(AtomicReference<Insight> capturedInsight) {
		return (IReactor) Proxy.newProxyInstance(IReactor.class.getClassLoader(), new Class<?>[] { IReactor.class },
				(proxy, method, arguments) -> {
					if ("setInsight".equals(method.getName())) {
						capturedInsight.set((Insight) arguments[0]);
					}
					return defaultValue(method.getReturnType());
				});
	}

	private static Object defaultValue(Class<?> returnType) {
		if (!returnType.isPrimitive() || returnType == void.class) {
			return null;
		}
		if (returnType == boolean.class) {
			return false;
		}
		if (returnType == char.class) {
			return '\0';
		}
		return 0;
	}

	private static Object invoke(PipelineInvocationHandler handler, String methodName, Class<?>[] parameterTypes,
			Object... arguments) throws Exception {
		Method method = PipelineInvocationHandler.class.getDeclaredMethod(methodName, parameterTypes);
		method.setAccessible(true);
		return method.invoke(handler, arguments);
	}
}
