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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IVenvEngine;
import prerna.project.api.IProject;

/**
 * Engines and projects for the access check's tests: each is a stand-in that
 * answers with empty values, behind the same proxy {@code Utility} hands out,
 * so a call on it runs the real access check.
 */
public final class StubEngines {

	private StubEngines() {
	}

	/**
	 * An RDBMS database behind the engine proxy.
	 *
	 * @param id the engine id
	 * @return the proxy
	 */
	public static IRDBMSEngine database(String id) {
		return database(id, Map.of());
	}

	/**
	 * An RDBMS database behind the engine proxy, some of whose methods run the
	 * test's own code.
	 *
	 * @param id      the engine id
	 * @param answers what a method does, by method name
	 * @return the proxy
	 */
	public static IRDBMSEngine database(String id, Map<String, Callable<Object>> answers) {
		IRDBMSEngine engine = stub(IRDBMSEngine.class, id, IEngine.CATALOG_TYPE.DATABASE, answers, IEngine.class,
				IDatabaseEngine.class);
		return (IRDBMSEngine) EngineProxyFactory.createGuardedDatabaseEngine(engine);
	}

	/**
	 * An RDBMS database with no proxy around it, as the platform keeps a loaded
	 * engine. A test that puts it where loaded engines are kept gets it back from
	 * {@code Utility.getDatabase} behind the real proxy.
	 *
	 * @param id      the engine id
	 * @param answers what a method does, by method name
	 * @return the engine itself
	 */
	public static IRDBMSEngine unguardedDatabase(String id, Map<String, Callable<Object>> answers) {
		return stub(IRDBMSEngine.class, id, IEngine.CATALOG_TYPE.DATABASE, answers, IEngine.class,
				IDatabaseEngine.class);
	}

	/**
	 * A model behind the engine proxy.
	 *
	 * @param id the engine id
	 * @return the proxy
	 */
	public static IModelEngine model(String id) {
		return model(id, Map.of());
	}

	/**
	 * A model behind the engine proxy, some of whose methods run the test's own
	 * code.
	 *
	 * @param id      the engine id
	 * @param answers what a method does, by method name
	 * @return the proxy
	 */
	public static IModelEngine model(String id, Map<String, Callable<Object>> answers) {
		IModelEngine engine = stub(IModelEngine.class, id, IEngine.CATALOG_TYPE.MODEL, answers, IEngine.class);
		return EngineProxyFactory.createGuardedModelEngine(engine);
	}

	/**
	 * A model with no proxy around it, as the platform keeps a loaded engine. A
	 * test that puts it where loaded engines are kept gets it back from
	 * {@code Utility.getModel} behind the real proxy, with the pipeline its
	 * settings name when an answer for {@code getSmssProp} gives one.
	 *
	 * @param id      the engine id
	 * @param answers what a method does, by method name
	 * @return the engine itself
	 */
	public static IModelEngine unguardedModel(String id, Map<String, Callable<Object>> answers) {
		return stub(IModelEngine.class, id, IEngine.CATALOG_TYPE.MODEL, answers, IEngine.class);
	}

	/**
	 * A model behind the proxy the platform hands out for its own use.
	 *
	 * @param id the engine id
	 * @return the proxy
	 */
	public static IModelEngine platformModel(String id) {
		IModelEngine engine = stub(IModelEngine.class, id, IEngine.CATALOG_TYPE.MODEL, Map.of(), IEngine.class);
		return EngineProxyFactory.createPlatformModelEngine(engine);
	}

	/**
	 * A guardrail behind the engine proxy, some of whose methods run the test's own
	 * code.
	 *
	 * @param id      the engine id
	 * @param answers what a method does, by method name
	 * @return the proxy
	 */
	public static IGuardrailReactorFunctionEngine guardrail(String id, Map<String, Callable<Object>> answers) {
		IGuardrailReactorFunctionEngine engine = stub(IGuardrailReactorFunctionEngine.class, id,
				IEngine.CATALOG_TYPE.GUARDRAIL, answers, IEngine.class, IFunctionEngine.class);
		return (IGuardrailReactorFunctionEngine) EngineProxyFactory.createGuardedFunctionEngine(engine);
	}

	/**
	 * A venv engine behind the engine proxy.
	 *
	 * @param id the engine id
	 * @return the proxy
	 */
	public static IVenvEngine venv(String id) {
		IVenvEngine engine = stub(IVenvEngine.class, id, IEngine.CATALOG_TYPE.VENV, Map.of(), IEngine.class);
		return EngineProxyFactory.createGuardedVenvEngine(engine);
	}

	/**
	 * A project behind the engine proxy.
	 *
	 * @param id the project id
	 * @return the proxy
	 */
	public static IProject project(String id) {
		IProject project = stub(IProject.class, id, IEngine.CATALOG_TYPE.PROJECT, Map.of(), IEngine.class);
		return EngineProxyFactory.createGuardedProject(project);
	}

	private static <T> T stub(Class<T> type, String id, IEngine.CATALOG_TYPE catalogType,
			Map<String, Callable<Object>> answers, Class<?>... others) {
		List<Class<?>> interfaces = new ArrayList<>(List.of(others));
		interfaces.add(type);
		Properties smss = new Properties();
		InvocationHandler handler = (proxy, method, arguments) -> {
			Callable<Object> answer = answers.get(method.getName());
			if (answer != null) {
				return answer.call();
			}
			return switch (method.getName()) {
			case "getEngineId", "getProjectId" -> id;
			case "getEngineName", "getProjectName" -> "Stub " + id;
			case "getCatalogType" -> catalogType;
			case "getSmssProp", "getOrigSmssProp" -> smss;
			case "toString" -> "stub " + id;
			case "hashCode" -> id.hashCode();
			case "equals" -> proxy == arguments[0];
			default -> emptyValue(method.getReturnType());
			};
		};
		return type.cast(
				Proxy.newProxyInstance(IEngine.class.getClassLoader(), interfaces.toArray(new Class<?>[0]), handler));
	}

	private static Object emptyValue(Class<?> type) {
		if (!type.isPrimitive() || type == void.class) {
			return null;
		}
		if (type == boolean.class) {
			return false;
		}
		if (type == char.class) {
			return '\0';
		}
		if (type == long.class) {
			return 0L;
		}
		if (type == float.class) {
			return 0f;
		}
		if (type == double.class) {
			return 0d;
		}
		if (type == byte.class) {
			return (byte) 0;
		}
		if (type == short.class) {
			return (short) 0;
		}
		return 0;
	}

}
