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

import org.javatuples.Pair;
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
	private static final String CONTAINS_URI = "http://semoss.org/ontologies/Relation/Contains/";

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
		when(queryUtil.getBlobDataTypeName()).thenReturn("BLOB");
		when(queryUtil.getClobDataTypeName()).thenReturn("CLOB");
		when(queryUtil.getBooleanDataTypeName()).thenReturn("BOOLEAN");
		when(queryUtil.getDateWithTimeDataType()).thenReturn("TIMESTAMP");
		when(queryUtil.getIntegerDataTypeName()).thenReturn("INT");
		when(queryUtil.getDoubleDataTypeName()).thenReturn("DOUBLE");

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
		List<String> concepts = schemaConcepts();
		List<String> props = new ArrayList<>();

		when(engine.getPhysicalConcepts()).thenReturn(concepts);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getInstanceName(anyString())).thenAnswer(inv -> inv.getArgument(0));

			when(engine.getPropertyUris4PhysicalUri(anyString())).thenReturn(props);

			assertTrue(reactor.needsRemake(engine));
		}
	}

	@Test
	void needsRemakeFalse() {
		List<String> concepts = schemaConcepts();
		List<String> props = schemaProperties();

		when(engine.getPhysicalConcepts()).thenReturn(concepts);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getInstanceName(anyString())).thenAnswer(inv -> inv.getArgument(0));

			when(engine.getPropertyUris4PhysicalUri(anyString())).thenReturn(props);

			assertFalse(reactor.needsRemake(engine));
		}
	}

	@Test
	void needsRemakeTrueWhenOneColumnMissing() {
		List<String> concepts = schemaConcepts();
		List<String> props = schemaProperties();
		props.remove(CONTAINS_URI + "COST_MICROS/MESSAGE");

		when(engine.getPhysicalConcepts()).thenReturn(concepts);

		try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {
			util.when(() -> Utility.getInstanceName(anyString())).thenAnswer(inv -> inv.getArgument(0));

			when(engine.getPropertyUris4PhysicalUri(anyString())).thenReturn(props);

			assertTrue(reactor.needsRemake(engine));
		}
	}

	@Test
	void remakeOwl() throws Exception {
		when(engine.getOWLEngineFactory()).thenReturn(owlFactory);
		when(owlFactory.getWriteOWL()).thenReturn(owlEngine);

		reactor.remakeOwl(engine);

		List<Pair<String, List<Pair<String, String>>>> schema = reactor.getDBSchema();
		int columnCount = 0;
		verify(owlEngine).createEmptyOWLFile();
		for (Pair<String, List<Pair<String, String>>> table : schema) {
			verify(owlEngine).addConcept(table.getValue0(), null, null);
			for (Pair<String, String> column : table.getValue1()) {
				verify(owlEngine).addProp(table.getValue0(), column.getValue0(), column.getValue1());
				columnCount++;
			}
		}
		verify(owlEngine, times(schema.size())).addConcept(anyString(), eq(null), eq(null));
		verify(owlEngine, times(columnCount)).addProp(anyString(), anyString(), anyString());
		verify(owlEngine).commit();
		verify(owlEngine).export();
	}

	/**
	 * Builds the physical concept list an up to date OWL reports: the base concept
	 * followed by every table the creator declares.
	 *
	 * @return the base concept URI and each declared table name
	 */
	private List<String> schemaConcepts() {
		List<String> concepts = new ArrayList<>();
		concepts.add("http://semoss.org/ontologies/Concept");
		for (Pair<String, List<Pair<String, String>>> table : reactor.getDBSchema()) {
			concepts.add(table.getValue0());
		}
		return concepts;
	}

	/**
	 * Builds the property URI list an up to date OWL reports for every declared
	 * column.
	 *
	 * @return one contains relation URI per declared table column
	 */
	private List<String> schemaProperties() {
		List<String> props = new ArrayList<>();
		for (Pair<String, List<Pair<String, String>>> table : reactor.getDBSchema()) {
			for (Pair<String, String> column : table.getValue1()) {
				props.add(CONTAINS_URI + column.getValue0() + "/" + table.getValue0());
			}
		}
		return props;
	}

	@Test
	void getters() {
		assertNotNull(reactor.getDBSchema());
	}
}
