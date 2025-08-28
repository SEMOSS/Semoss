/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.pipeline;

import java.io.File;
import java.lang.reflect.Proxy;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
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
	 * @param engine
	 * @return
	 */
	public static IModelEngine createGuardedModelEngine(IModelEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IModelEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IModelEngine.class}, handler);
				}
			}
		}

		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IDatabaseEngine createGuardedDatabaseEngine(IDatabaseEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					Class<?>[] classes = null;
					if (engine instanceof IRDBMSEngine) {
						classes = new Class<?>[]{IEngine.class, IDatabaseEngine.class, IRDBMSEngine.class};
					} else {
						classes = new Class<?>[]{IEngine.class, IDatabaseEngine.class};
					}
					return (IDatabaseEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(), classes, handler);
				}
			}
		}
		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IStorageEngine createGuardedStorageEngine(IStorageEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IStorageEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IStorageEngine.class}, handler);
				}
			}
		}
		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IFunctionEngine createGuardedFunctionEngine(IFunctionEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IFunctionEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IFunctionEngine.class}, handler);
				}
			}
		}
		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IGuardrailReactorFunctionEngine createGuardedGuardrailEngine(IGuardrailReactorFunctionEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IGuardrailReactorFunctionEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IGuardrailReactorFunctionEngine.class}, handler);
				}
			}
		}
		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IReactorFunctionEngine createGuardedReactorEngine(IReactorFunctionEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IReactorFunctionEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IReactorFunctionEngine.class}, handler);
				}
			}
		}
		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IVectorDatabaseEngine createGuardedVectorEngine(IVectorDatabaseEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IVectorDatabaseEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IVectorDatabaseEngine.class}, handler);
				}
			}
		}

		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IProject createGuardedProject(IProject engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IProject) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IProject.class}, handler);
				}
			}
		}

		return engine;
	}

	/**
	 * @param engine
	 * @return
	 */
	public static IVenvEngine createGuardedVenvEngine(IVenvEngine engine) {
		if (engine != null && engine.getSmssProp().containsKey(IEngine.PIPELINE)) {
			String pipelineValue = engine.getSmssProp().getProperty(IEngine.PIPELINE);
			if (pipelineValue != null && !pipelineValue.isBlank()) {
				File jsonFile = getJsonFile(engine, pipelineValue);
				if (jsonFile.exists() && jsonFile.isFile()) {
					PipelineInvocationHandler handler = new PipelineInvocationHandler(engine, jsonFile);
					return (IVenvEngine) Proxy.newProxyInstance(IEngine.class.getClassLoader(),
							new Class<?>[]{IEngine.class, IVenvEngine.class}, handler);
				}
			}
		}

		return engine;
	}

	/**
	 * @param engine
	 * @param pipeline
	 * @return
	 */
	private static File getJsonFile(IEngine engine, String pipeline) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		String pipelineFile = assetsFolder + "/" + engine.getSmssProp().getProperty(IEngine.PIPELINE);
		pipelineFile = pipelineFile.replace("\\", "/");
		File jsonFile = new File(pipelineFile);
		return jsonFile;
	}
}
