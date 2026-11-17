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
package prerna.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import prerna.reactor.agent.skill.CloneSkillReactor;
import prerna.reactor.agent.skill.CreateSkillReactor;
import prerna.reactor.automation.project.CreateAutomationReactor;
import prerna.reactor.database.upload.rdbms.external.CreateNewRdbmsExternalDatabaseReactor;
import prerna.reactor.database.upload.rdbms.internal.CreateEmptyRdbmsDatabaseReactor;
import prerna.reactor.database.upload.rdbms.internal.CreateNewRdbmsInternalDatabaseReactor;
import prerna.reactor.engine.CloneEngineReactor;
import prerna.reactor.function.upload.CreateFunctionEngineReactor;
import prerna.reactor.function.upload.CreatePythonFunctionEngineReactor;
import prerna.reactor.guardrail.upload.CreateGuardrailEngineReactor;
import prerna.reactor.model.upload.CreateModelEngineReactor;
import prerna.reactor.project.CreateAppFromBlocksReactor;
import prerna.reactor.project.CreateAppFromTemplateReactor;
import prerna.reactor.project.CreateNotebookReactor;
import prerna.reactor.project.CreateProjectReactor;
import prerna.reactor.storage.upload.CreateStorageEngineReactor;
import prerna.reactor.vector.upload.CreateVectorDatabaseEngineReactor;
import prerna.reactor.venv.upload.CreateVenvEngineReactor;
import prerna.sablecc2.om.ReactorKeysEnum;

class CatalogNameKeyUnitTests {

	private static final String CATALOG_NAME = ReactorKeysEnum.CATALOG_NAME.getKey();

	static Stream<Arguments> createReactors() {
		return Stream.of(Arguments.of(new CreateModelEngineReactor(), "model"),
				Arguments.of(new CreateStorageEngineReactor(), "storage"),
				Arguments.of(new CreateVectorDatabaseEngineReactor(), "database"),
				Arguments.of(new CreateFunctionEngineReactor(), "function"),
				Arguments.of(new CreatePythonFunctionEngineReactor(), "function"),
				Arguments.of(new CreateGuardrailEngineReactor(), "guardrail"),
				Arguments.of(new CreateVenvEngineReactor(), "venv"),
				Arguments.of(new CreateEmptyRdbmsDatabaseReactor(), "database"),
				Arguments.of(new CreateNewRdbmsExternalDatabaseReactor(), "database"),
				Arguments.of(new CreateNewRdbmsInternalDatabaseReactor(), "database"),
				Arguments.of(new CreateProjectReactor(), "project"),
				Arguments.of(new CreateAppFromBlocksReactor(), "project"),
				Arguments.of(new CreateAppFromTemplateReactor(), "project"),
				Arguments.of(new CreateNotebookReactor(), "project"),
				Arguments.of(new CreateAutomationReactor(), "projectName"),
				Arguments.of(new CreateSkillReactor(), "name"),
				Arguments.of(new CloneSkillReactor(), "name"),
				Arguments.of(new CloneEngineReactor(), "name"));
	}

	@ParameterizedTest
	@MethodSource("createReactors")
	void describesOnlyCatalogNameAndStillTakesTheKeyItReplaced(AbstractReactor reactor, String replacedKey) {
		ReactorDefinition definition = reactor.getReactorDefinition();
		ReactorDefinition.Key catalogName = definition.getKey(CATALOG_NAME);
		assertNotNull(catalogName, reactor.getClass().getSimpleName() + " has no " + CATALOG_NAME + " key");
		assertEquals(List.of(replacedKey), catalogName.getAliases());
		assertNull(definition.getKey(replacedKey));
	}

	@ParameterizedTest
	@MethodSource("createReactors")
	void definesNoProblems(AbstractReactor reactor, String replacedKey) {
		assertEquals(List.of(), reactor.getReactorDefinition().getProblems());
	}

	@Test
	void cloneEngineDescribesOnlyEngineIdAndStillTakesEngine() {
		ReactorDefinition definition = new CloneEngineReactor().getReactorDefinition();
		ReactorDefinition.Key source = definition.getKey("engineId");
		assertNotNull(source);
		assertEquals(List.of("engine"), source.getAliases());
		assertNull(definition.getKey("engine"));
	}
}
