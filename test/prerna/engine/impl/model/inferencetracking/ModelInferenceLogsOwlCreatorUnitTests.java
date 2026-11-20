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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.OWLEngineFactory;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsOwlCreatorUnitTests {
	IRDBMSEngine engine;
	WriteOWLEngine owlEngine;
	OWLEngineFactory owlFactory;
	AbstractSqlQueryUtil queryUtil;
	ModelInferenceLogsOwlCreator reactor;

	@BeforeEach
	void setup() {
		engine = mock(IRDBMSEngine.class);
		owlEngine = mock(WriteOWLEngine.class);
		owlFactory = mock(OWLEngineFactory.class);
		queryUtil = mock(AbstractSqlQueryUtil.class);

		when(engine.getQueryUtil()).thenReturn(queryUtil);

		reactor = new ModelInferenceLogsOwlCreator(engine.getQueryUtil());
	}

	@Test
	void needsRemakeTrue() {
		List<String> concepts = new ArrayList<>();
		concepts.add("http://semoss.org/ontologies/Concept");
		concepts.add("AGENT");

		when(engine.getPhysicalConcepts()).thenReturn(concepts);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getInstanceName("AGENT")).thenReturn("AGENT");

			assertTrue(reactor.needsRemake(engine));
		}
	}

	@Test
	void needsRemakeTrue2() {
		List<String> concepts = new ArrayList<>();
		concepts.add("http://semoss.org/ontologies/Concept");
		concepts.add("AGENT");
		concepts.add("ROOM");
		concepts.add("MESSAGE");
		concepts.add("FEEDBACK");
		concepts.add("WORKSPACE");
		concepts.add("WORKSPACE_RESOURCE");

		List<String> props = new ArrayList<>();

		when(engine.getPhysicalConcepts()).thenReturn(concepts);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getInstanceName("AGENT")).thenReturn("AGENT");
			util.when(() -> Utility.getInstanceName("ROOM")).thenReturn("ROOM");
			util.when(() -> Utility.getInstanceName("MESSAGE")).thenReturn("MESSAGE");
			util.when(() -> Utility.getInstanceName("FEEDBACK")).thenReturn("FEEDBACK");
			util.when(() -> Utility.getInstanceName("WORKSPACE")).thenReturn("WORKSPACE");
			util.when(() -> Utility.getInstanceName("WORKSPACE_RESOURCE")).thenReturn("WORKSPACE_RESOURCE");

			when(engine.getPropertyUris4PhysicalUri(anyString())).thenReturn(props);

			assertTrue(reactor.needsRemake(engine));
		}
	}

	@Test
	void needsRemakeFalse() {
		List<String> concepts = new ArrayList<>();
		concepts.add("http://semoss.org/ontologies/Concept");
		concepts.add("AGENT");
		concepts.add("ROOM");
		concepts.add("MESSAGE");
		concepts.add("FEEDBACK");
		concepts.add("WORKSPACE");
		concepts.add("WORKSPACE_RESOURCE");

		List<String> props = new ArrayList<>();
		props.add("http://semoss.org/ontologies/Relation/Contains/AGENT_ID/AGENT");
		props.add("http://semoss.org/ontologies/Relation/Contains/AGENT_NAME/AGENT");
		props.add("http://semoss.org/ontologies/Relation/Contains/DESCRIPTION/AGENT");
		props.add("http://semoss.org/ontologies/Relation/Contains/AGENT_TYPE/AGENT");
		props.add("http://semoss.org/ontologies/Relation/Contains/AUTHOR/AGENT");
		props.add("http://semoss.org/ontologies/Relation/Contains/DATE_CREATED/AGENT");

		props.add("http://semoss.org/ontologies/Relation/Contains/INSIGHT_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/ROOM_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/ROOM_NAME/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/ROOM_CONTEXT/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/USER_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/USER_NAME/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/USER_EMAIL_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/AGENT_TYPE/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/AGENT_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/IS_ACTIVE/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/DATE_CREATED/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/UPDATED_AT/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/PROJECT_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/PROJECT_NAME/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/MODEL_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGES/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/PINNED/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/OPTIONS/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/SHARE_ID/ROOM");
		props.add("http://semoss.org/ontologies/Relation/Contains/WORKSPACE_ID/ROOM");

		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGE_ID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGE_TYPE/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGE_DATA/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGE_TOKENS/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGE_METHOD/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/RESPONSE_TIME/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/DATE_CREATED/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/AGENT_ID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/MODEL_ID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/INSIGHT_ID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/ROOM_ID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/SESSIONID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/USER_ID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/USER_NAME/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/USER_EMAIL_ID/MESSAGE");
		props.add("http://semoss.org/ontologies/Relation/Contains/TRANSACTION_ID/MESSAGE");

		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGE_ID/FEEDBACK");
		props.add("http://semoss.org/ontologies/Relation/Contains/MESSAGE_TYPE/FEEDBACK");
		props.add("http://semoss.org/ontologies/Relation/Contains/FEEDBACK_TEXT/FEEDBACK");
		props.add("http://semoss.org/ontologies/Relation/Contains/FEEDBACK_DATE/FEEDBACK");
		props.add("http://semoss.org/ontologies/Relation/Contains/RATING/FEEDBACK");

		props.add("http://semoss.org/ontologies/Relation/Contains/WORKSPACE_ID/WORKSPACE");
		props.add("http://semoss.org/ontologies/Relation/Contains/OWNER/WORKSPACE");
		props.add("http://semoss.org/ontologies/Relation/Contains/NAME/WORKSPACE");
		props.add("http://semoss.org/ontologies/Relation/Contains/DESCRIPTION/WORKSPACE");
		props.add("http://semoss.org/ontologies/Relation/Contains/SYSTEM_PROMPT/WORKSPACE");
		props.add("http://semoss.org/ontologies/Relation/Contains/IS_ACTIVE/WORKSPACE");
		props.add("http://semoss.org/ontologies/Relation/Contains/DATE_CREATED/WORKSPACE");
		props.add("http://semoss.org/ontologies/Relation/Contains/DATE_UPDATED/WORKSPACE");

		props.add("http://semoss.org/ontologies/Relation/Contains/WORKSPACE_RESOURCE_ID/WORKSPACE_RESOURCE");
		props.add("http://semoss.org/ontologies/Relation/Contains/WORKSPACE_ID/WORKSPACE_RESOURCE");
		props.add("http://semoss.org/ontologies/Relation/Contains/RESOURCE_ID/WORKSPACE_RESOURCE");
		props.add("http://semoss.org/ontologies/Relation/Contains/RESOURCE_TYPE/WORKSPACE_RESOURCE");
		props.add("http://semoss.org/ontologies/Relation/Contains/RESOURCE_SUBTYPE/WORKSPACE_RESOURCE");

		when(engine.getPhysicalConcepts()).thenReturn(concepts);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getInstanceName("AGENT")).thenReturn("AGENT");
			util.when(() -> Utility.getInstanceName("ROOM")).thenReturn("ROOM");
			util.when(() -> Utility.getInstanceName("MESSAGE")).thenReturn("MESSAGE");
			util.when(() -> Utility.getInstanceName("FEEDBACK")).thenReturn("FEEDBACK");
			util.when(() -> Utility.getInstanceName("WORKSPACE")).thenReturn("WORKSPACE");
			util.when(() -> Utility.getInstanceName("WORKSPACE_RESOURCE")).thenReturn("WORKSPACE_RESOURCE");

			when(engine.getPropertyUris4PhysicalUri(anyString())).thenReturn(props);

			assertFalse(reactor.needsRemake(engine));
		}
	}

	@Test
	void remakeOwl() throws Exception {
		when(engine.getOWLEngineFactory()).thenReturn(owlFactory);
		when(owlFactory.getWriteOWL()).thenReturn(owlEngine);

		reactor.remakeOwl(engine);

		verify(owlEngine).createEmptyOWLFile();
		verify(owlEngine, times(6)).addConcept(anyString(), eq(null), eq(null));
		verify(owlEngine, times(40)).addProp(anyString(), anyString(), anyString());
		verify(owlEngine).commit();
		verify(owlEngine).export();
	}

	@Test
	void getters() {
		assertNotNull(reactor.getDBSchema());
	}
}
