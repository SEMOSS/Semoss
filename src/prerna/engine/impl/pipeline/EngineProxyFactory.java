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

import java.io.File;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEmbeddedRDBMSServerEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.api.IRCloneStorage;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRDFDatabase;
import prerna.engine.api.IReactorFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.api.IVenvEngine;
import prerna.project.api.IProject;
import prerna.util.EngineUtility;

/**
 * Factory for creating a dynamic proxy that wraps an IEngine instance to apply
 * input and output processing pipelines.
 */
public class EngineProxyFactory {

	/**
	 * 
	 * @param engine
	 * @return
	 */
	public static IModelEngine createGuardedModelEngine(IModelEngine engine) {
		if (engine == null) {
			return null;
		}

		File jsonFile = null;
		if (engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				jsonFile = getJsonFile(engine, pipelineValue);
			}
		}

		PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
		Class<?>[] classes = null;
		if (engine instanceof IModelRouterEngine) {
			classes = new Class<?>[] { IEngine.class, IModelEngine.class, IModelRouterEngine.class };
		} else {
			classes = new Class<?>[] { IEngine.class, IModelEngine.class };
		}
		return (IModelEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(), classes, handler);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	public static IDatabaseEngine createGuardedDatabaseEngine(IDatabaseEngine engine) {
		if (engine == null) {
			return null;
		}

		File jsonFile = null;
		if (engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				jsonFile = getJsonFile(engine, pipelineValue);
			}
		}

		PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
		Class<?>[] classes = null;
		if (engine instanceof IEmbeddedRDBMSServerEngine) {
			classes = new Class<?>[] { IEngine.class, IDatabaseEngine.class, IRDBMSEngine.class,
					IEmbeddedRDBMSServerEngine.class };
		} else if (engine instanceof IRDBMSEngine) {
			classes = new Class<?>[] { IEngine.class, IDatabaseEngine.class, IRDBMSEngine.class };
		} else if (engine instanceof IRDFDatabase) {
			classes = new Class<?>[] { IEngine.class, IDatabaseEngine.class, IRDFDatabase.class };
		} else {
			classes = new Class<?>[] { IEngine.class, IDatabaseEngine.class };
		}
		return (IDatabaseEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(), classes, handler);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	public static IStorageEngine createGuardedStorageEngine(IStorageEngine engine) {
		if (engine == null) {
			return null;
		}

		File jsonFile = null;
		if (engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				jsonFile = getJsonFile(engine, pipelineValue);
			}
		}

		PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
		Class<?>[] classes = null;
		if (engine instanceof IRCloneStorage) {
			classes = new Class<?>[] { IEngine.class, IStorageEngine.class, IRCloneStorage.class };
		} else {
			classes = new Class<?>[] { IEngine.class, IStorageEngine.class };
		}
		return (IStorageEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(), classes, handler);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	public static IFunctionEngine createGuardedFunctionEngine(IFunctionEngine engine) {
		if (engine == null) {
			return null;
		}

		File jsonFile = null;
		if (engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				jsonFile = getJsonFile(engine, pipelineValue);
			}
		}

		List<Class<?>> classes = new ArrayList<>();
		classes.add(IEngine.class);
		classes.add(IFunctionEngine.class);
		if (engine instanceof IReactorFunctionEngine) {
			classes.add(IReactorFunctionEngine.class);
		}
		if (engine instanceof IGuardrailReactorFunctionEngine) {
			classes.add(IGuardrailReactorFunctionEngine.class);
		}
		if (engine instanceof ICustomEmbeddingsFunctionEngine) {
			classes.add(ICustomEmbeddingsFunctionEngine.class);
		}

		PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
		return (IFunctionEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
				classes.toArray(new Class<?>[0]), handler);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	public static IVectorDatabaseEngine createGuardedVectorEngine(IVectorDatabaseEngine engine) {
		if (engine == null) {
			return null;
		}

		File jsonFile = null;
		if (engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				jsonFile = getJsonFile(engine, pipelineValue);
			}
		}

		PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
		return (IVectorDatabaseEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
				new Class<?>[] { IEngine.class, IVectorDatabaseEngine.class }, handler);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	public static IProject createGuardedProject(IProject engine) {
		if (engine == null) {
			return null;
		}

		File jsonFile = null;
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				jsonFile = getJsonFile(engine, pipelineValue);
			}
		}

		PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
		return (IProject) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
				new Class<?>[] { IEngine.class, IProject.class }, handler);
	}

	/**
	 * 
	 * @param engine
	 * @return
	 */
	public static IVenvEngine createGuardedVenvEngine(IVenvEngine engine) {
		if (engine == null) {
			return null;
		}

		File jsonFile = null;
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				jsonFile = getJsonFile(engine, pipelineValue);
			}
		}

		PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
		return (IVenvEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
				new Class<?>[] { IEngine.class, IVenvEngine.class }, handler);
	}

	/**
	 * 
	 * @param engine
	 * @param pipeline
	 * @return
	 */
	private static File getJsonFile(IEngine engine, String pipeline) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String pipelineFile = assetsFolder + "/" + pipeline.trim();
		pipelineFile = pipelineFile.replace("\\", "/");
		File jsonFile = new File(pipelineFile);
		return jsonFile;
	}

}
