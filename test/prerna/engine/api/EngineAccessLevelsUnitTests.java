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
package prerna.engine.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import prerna.auth.AccessPermissionEnum;
import prerna.project.api.IProject;

/**
 * The permission level every method of the engine and project interfaces
 * carries, which the engine proxy checks on each call. A method added to one of
 * these interfaces with no level fails here, so no method reaches an engine
 * unchecked.
 */
class EngineAccessLevelsUnitTests {

	private static final List<Class<?>> PROXIED = List.of(IEngine.class, IExplorable.class, IDatabaseEngine.class,
			IRDBMSEngine.class, IEmbeddedRDBMSServerEngine.class, IRDFDatabase.class, IModelEngine.class,
			IModelRouterEngine.class, ITypeSafeEngine.class, IOpenAIDecisionsEngine.class, IStorageEngine.class,
			IRCloneStorage.class, IFunctionEngine.class, IReactorFunctionEngine.class,
			IGuardrailReactorFunctionEngine.class, ICustomEmbeddingsFunctionEngine.class,
			IVectorDatabaseEngine.class, IVenvEngine.class, IMCP.class, IProject.class);

	@Test
	void everyMethodOfAnEngineOrProjectInterfaceHasALevel() {
		List<String> missing = new ArrayList<>();
		for (Class<?> type : PROXIED) {
			for (Method method : type.getDeclaredMethods()) {
				if (!Modifier.isStatic(method.getModifiers()) && !method.isSynthetic()
						&& method.getAnnotation(EngineAccess.class) == null) {
					missing.add(type.getSimpleName() + "." + method.getName());
				}
			}
		}

		assertEquals(List.of(), missing);
	}

	@Test
	void theLevelForProjectCodeIsOneLevelAboveTheLevelOfEveryCall() {
		for (Class<?> type : PROXIED) {
			for (Method method : type.getDeclaredMethods()) {
				EngineAccess access = method.getAnnotation(EngineAccess.class);
				if (access == null) {
					continue;
				}
				String name = type.getSimpleName() + "." + method.getName();
				assertNotNull(access.value(), name);
				assertTrue(access.projectCode().length <= 1, name);
				if (access.projectCode().length == 1) {
					assertTrue(access.projectCode()[0].getId() < access.value().getId(),
							name + " gives project code a level that is not above the level of every call");
				}
			}
		}
	}

	@ParameterizedTest
	@CsvSource({ "prerna.engine.api.IEngine,getSmssProp,OWNER,OWNER",
			"prerna.engine.api.IEngine,getOrigSmssProp,OWNER,OWNER",
			"prerna.engine.api.IEngine,getSmssFilePath,OWNER,OWNER",
			"prerna.engine.api.IEngine,setSmssProp,OWNER,OWNER", "prerna.engine.api.IEngine,open,OWNER,OWNER",
			"prerna.engine.api.IEngine,delete,OWNER,OWNER", "prerna.engine.api.IEngine,setEngineId,OWNER,OWNER",
			"prerna.engine.api.IEngine,getEngineId,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IEngine,reload,EDIT,OWNER", "prerna.engine.api.IEngine,updateSetting,EDIT,OWNER",
			"prerna.engine.api.IEngine,getCatalogSubType,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IEngine,getPipelineFile,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IModelEngine,getModelName,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IModelEngine,getMaxToolNameLength,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IVectorDatabaseEngine,getEmbedderEngineId,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IStorageEngine,getBucketName,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IDatabaseEngine,isBigData,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IFunctionEngine,getFunctionType,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IGuardrailReactorFunctionEngine,getJudgeModelEngineId,READ_ONLY,READ_ONLY",
			"prerna.project.api.IProject,getSandboxSecretSettings,READ_ONLY,OWNER",
			"prerna.engine.api.IExplorable,getProperty,OWNER,OWNER",
			"prerna.engine.api.IExplorable,addProperty,OWNER,OWNER",
			"prerna.engine.api.IExplorable,commitOWL,EDIT,EDIT",
			"prerna.engine.api.IExplorable,setOwlFilePath,OWNER,OWNER",
			"prerna.engine.api.IExplorable,setBaseDataEngine,EDIT,OWNER",
			"prerna.project.api.IProject,setInsightDatabase,EDIT,OWNER",
			"prerna.engine.api.IVenvEngine,pullRequirementsFile,EDIT,EDIT",
			"prerna.engine.api.IDatabaseEngine,execQuery,READ_ONLY,EDIT",
			"prerna.engine.api.IDatabaseEngine,insertData,EDIT,EDIT",
			"prerna.engine.api.IDatabaseEngine,removeData,EDIT,EDIT",
			"prerna.engine.api.IDatabaseEngine,commit,EDIT,EDIT",
			"prerna.engine.api.IRDBMSEngine,getConnection,READ_ONLY,EDIT",
			"prerna.engine.api.IRDBMSEngine,getPreparedStatement,READ_ONLY,EDIT",
			"prerna.engine.api.IRDBMSEngine,getQueryUtil,READ_ONLY,EDIT",
			"prerna.engine.api.IRDBMSEngine,getDataSource,OWNER,OWNER",
			"prerna.engine.api.IRDBMSEngine,getConnectionUrl,OWNER,OWNER",
			"prerna.engine.api.IRDBMSEngine,bulkInsertPreparedStatement,EDIT,EDIT",
			"prerna.engine.api.IModelEngine,ask,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IModelEngine,embeddings,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IModelRouterEngine,updateConfig,EDIT,EDIT",
			"prerna.engine.api.IStorageEngine,list,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IStorageEngine,copyToLocal,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IStorageEngine,copyToStorage,EDIT,EDIT",
			"prerna.engine.api.IStorageEngine,deleteFromStorage,EDIT,EDIT",
			"prerna.engine.api.IVectorDatabaseEngine,nearestNeighbor,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IVectorDatabaseEngine,addDocument,EDIT,EDIT",
			"prerna.engine.api.IVectorDatabaseEngine,removeDocument,EDIT,EDIT",
			"prerna.engine.api.IFunctionEngine,execute,READ_ONLY,READ_ONLY",
			"prerna.engine.api.IVenvEngine,addPackage,EDIT,EDIT",
			"prerna.project.api.IProject,getReactor,READ_ONLY,READ_ONLY",
			"prerna.project.api.IProject,getInsightDatabase,READ_ONLY,EDIT",
			"prerna.project.api.IProject,getProjectProperties,READ_ONLY,EDIT",
			"prerna.project.api.IProject,getProjectPyTranslator,READ_ONLY,EDIT",
			"prerna.project.api.IProject,setProjectId,OWNER,OWNER" })
	void theMethodsThatMatterMostKeepTheirLevels(String type, String name, AccessPermissionEnum everyCall,
			AccessPermissionEnum projectCode) throws ClassNotFoundException {
		int found = 0;
		for (Method method : Class.forName(type).getDeclaredMethods()) {
			if (method.getName().equals(name)) {
				EngineAccess access = method.getAnnotation(EngineAccess.class);
				assertEquals(everyCall, access.value(), name);
				assertEquals(projectCode, access.projectCode().length == 0 ? access.value() : access.projectCode()[0],
						name);
				found++;
			}
		}

		assertTrue(found > 0, "No method named " + name + " in " + type);
	}

}
