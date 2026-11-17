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
package prerna.ds;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.cache.CachePropFileFrameObject;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;

/**
 * Tests TinkerFrame on a small graph of people, their pets and their ages,
 * where each person links to one pet and one age and two people share an age.
 */
class TinkerFrameUnitTests {

	private static final String FRAME_NAME = "people";
	private static final String PERSON = "Person";
	private static final String PET = "Pet";
	private static final String AGE = "Age";

	private TinkerFrame frame;

	@BeforeEach
	void setUp() {
		frame = new TinkerFrame(FRAME_NAME);
		defineColumn(PERSON, "STRING");
		defineColumn(PET, "STRING");
		defineColumn(AGE, "DOUBLE");
		frame.getMetaData().addRelationship(PERSON, PET, "inner.join");
		frame.getMetaData().addRelationship(PERSON, AGE, "inner.join");
		frame.syncHeaders();

		addPerson("Bob", "Max", 30.0);
		addPerson("Joe", "Bingo", 40.0);
		addPerson("Ann", "Rex", 30.0);
	}

	@AfterEach
	void tearDown() {
		if (!frame.isClosed()) {
			frame.close();
		}
	}

	private void defineColumn(String name, String type) {
		frame.getMetaData().addVertex(name);
		frame.getMetaData().setQueryStructNameToVertex(name, "test", name);
		frame.getMetaData().setAliasToVertex(name, name);
		frame.getMetaData().setDataTypeToVertex(name, type);
	}

	private void addPerson(String person, String pet, double age) {
		Map<Integer, Set<Integer>> cardinality = new HashMap<>();
		cardinality.put(0, new HashSet<>(Arrays.asList(1, 2)));
		frame.addRelationship(new String[] { PERSON, PET, AGE }, new Object[] { person, pet, age }, cardinality);
	}

	private SelectQueryStruct selectPeople() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(PERSON));
		qs.addSelector(new QueryColumnSelector(PET));
		qs.addSelector(new QueryColumnSelector(AGE));
		qs.addRelation(PERSON, PET, "inner.join");
		qs.addRelation(PERSON, AGE, "inner.join");
		return qs;
	}

	private static Set<List<Object>> collectRows(Iterator<IHeadersDataRow> it) {
		Set<List<Object>> rows = new HashSet<>();
		while (it.hasNext()) {
			rows.add(Arrays.asList(it.next().getValues()));
		}
		return rows;
	}

	private static Set<List<Object>> rowsOf(Object[]... rows) {
		Set<List<Object>> set = new HashSet<>();
		for (Object[] row : rows) {
			set.add(Arrays.asList(row));
		}
		return set;
	}

	private long vertexCount() {
		return frame.g.traversal().V().count().next();
	}

	private long vertexCount(String type) {
		return frame.g.traversal().V().has(TinkerFrame.TINKER_TYPE, type).count().next();
	}

	private Vertex vertex(String type, Object value) {
		return frame.g.traversal().V().has(TinkerFrame.TINKER_ID, type + ":" + value).next();
	}

	@Nested
	class Construction {

		@Test
		void defaultConstructor_generatesTinkerName() {
			TinkerFrame tinker = new TinkerFrame();
			try {
				assertTrue(tinker.getName().startsWith("TINKER_"));
				assertEquals(tinker.getName(), tinker.getOriginalName());
				assertTrue(tinker.isEmpty());
			} finally {
				tinker.close();
			}
		}

		@Test
		void defaultConstructor_generatesUniqueNames() {
			TinkerFrame first = new TinkerFrame();
			TinkerFrame second = new TinkerFrame();
			try {
				assertNotEquals(first.getName(), second.getName());
			} finally {
				first.close();
				second.close();
			}
		}

		@Test
		void aliasConstructor_usesAlias() {
			assertEquals(FRAME_NAME, frame.getName());
			assertEquals(FRAME_NAME, frame.getOriginalName());
		}

		@Test
		void headerConstructor_keepsHeaders() {
			String[] headers = { PERSON, PET };
			TinkerFrame tinker = new TinkerFrame(headers);
			try {
				assertArrayEquals(headers, tinker.getQsHeaders());
				assertTrue(tinker.isEmpty());
			} finally {
				tinker.close();
			}
		}

		@Test
		void frameType_isGraph() {
			assertEquals(DataFrameTypeEnum.GRAPH, frame.getFrameType());
		}

		@Test
		void queryInterpreter_isGremlin() {
			assertInstanceOf(GremlinInterpreter.class, frame.getQueryInterpreter());
		}

		@Test
		void iGraphSynched_isSettable() {
			assertFalse(frame.isIGraphSynched());
			frame.setIGraphSynched(true);
			assertTrue(frame.isIGraphSynched());
		}
	}

	@Nested
	class AddingData {

		@Test
		void addRelationship_createsOneVertexPerDistinctValue() {
			assertFalse(frame.isEmpty());
			assertEquals(3, vertexCount(PERSON));
			assertEquals(3, vertexCount(PET));
			assertEquals(2, vertexCount(AGE));
			assertEquals(8, vertexCount());
			assertEquals(6, frame.g.traversal().E().count().next());
		}

		@Test
		void addRelationship_labelsEdgesWithBothColumns() {
			Edge edge = vertex(PERSON, "Bob").edges(Direction.OUT, PERSON + TinkerFrame.EDGE_LABEL_DELIMETER + PET)
					.next();
			Integer count = edge.value(TinkerFrame.TINKER_COUNT);
			assertEquals("Max", edge.inVertex().value(TinkerFrame.TINKER_NAME));
			assertEquals(1, count.intValue());
		}

		@Test
		void addRelationship_repeatedRowCountsTheEdgeAgain() {
			addPerson("Bob", "Max", 30.0);

			assertEquals(8, vertexCount());
			Edge edge = vertex(PERSON, "Bob").edges(Direction.OUT, PERSON + TinkerFrame.EDGE_LABEL_DELIMETER + PET)
					.next();
			Integer count = edge.value(TinkerFrame.TINKER_COUNT);
			assertEquals(2, count.intValue());
		}

		@Test
		void addRelationship_keepsNumbersAsNumbers() {
			Object age = vertex(AGE, 30.0).value(TinkerFrame.TINKER_NAME);
			assertEquals(30.0, age);
		}

		@Test
		void addRelationship_withoutRelationshipsAddsALoneVertex() {
			frame.addRelationship(new String[] { PERSON }, new Object[] { "Zed" }, Collections.emptyMap());

			assertEquals(4, vertexCount(PERSON));
			assertTrue(frame.isOrphan(PERSON, "Zed"));
		}

		@Test
		void addRelationship_nullValueBecomesEmptyVertex() {
			frame.addRelationship(new String[] { PERSON }, new Object[] { null }, Collections.emptyMap());

			assertEquals("_", vertex(PERSON, TinkerFrame.EMPTY).value(TinkerFrame.TINKER_NAME));
		}

		@Test
		void addRelationship_withTypeMapNamesTheEdge() {
			Map<String, String> typeMap = new HashMap<>();
			typeMap.put(PERSON, "Owner");
			typeMap.put(PET, "Animal");
			Map<Integer, Set<Integer>> cardinality = new HashMap<>();
			cardinality.put(0, new HashSet<>(Collections.singletonList(1)));

			frame.addRelationship(new String[] { PERSON, PET }, new Object[] { "Kim", "Tom" }, cardinality, typeMap);

			assertTrue(vertex(PERSON, "Kim").edges(Direction.OUT, "Owner" + TinkerFrame.EDGE_LABEL_DELIMETER + "Animal")
					.hasNext());
		}
	}

	@Nested
	class Querying {

		@Test
		void query_returnsEveryRow() throws Exception {
			Set<List<Object>> rows = collectRows(frame.query(selectPeople()));

			assertEquals(rowsOf(new Object[] { "Bob", "Max", 30.0 }, new Object[] { "Joe", "Bingo", 40.0 },
					new Object[] { "Ann", "Rex", 30.0 }), rows);
		}

		@Test
		void query_appliesFilters() throws Exception {
			SelectQueryStruct qs = selectPeople();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(PET, "==", "Max"));

			Set<List<Object>> rows = collectRows(frame.query(qs));

			assertEquals(rowsOf(new Object[] { "Bob", "Max", 30.0 }), rows);
		}

		@Test
		void queryString_isNotSupported() {
			assertNull(frame.query("g.V()"));
		}

		@Test
		void getData_returnsEveryRow() {
			assertEquals(3, frame.getData().size());
		}

		@Test
		void size_countsRows() {
			assertEquals(3, frame.size(FRAME_NAME));
		}

		@Test
		void size_isZeroWithoutColumns() {
			TinkerFrame tinker = new TinkerFrame();
			try {
				assertEquals(0, tinker.size(tinker.getName()));
			} finally {
				tinker.close();
			}
		}

		@Test
		void isUniqueColumn_detectsDuplicates() {
			assertTrue(frame.isUniqueColumn(PERSON));
			assertFalse(frame.isUniqueColumn(AGE));
		}

		@Test
		void getColumnAsNumeric_returnsTheNumbers() {
			Double[] ages = frame.getColumnAsNumeric(AGE);

			assertNotNull(ages);
			assertEquals(new HashSet<>(Arrays.asList(30.0, 40.0)), new HashSet<>(Arrays.asList(ages)));
		}

		@Test
		void getColumnAsNumeric_isNullForText() {
			assertNull(frame.getColumnAsNumeric(PERSON));
		}

		@Test
		void maxAndMin_ofNumericColumn() {
			assertEquals(40.0, frame.getMax(AGE));
			assertEquals(30.0, frame.getMin(AGE));
		}
	}

	@Nested
	class GraphMetrics {

		@Test
		void degree_countsEdgesOfTheVertex() {
			assertEquals(2L, frame.degree(PERSON, "Bob"));
			assertEquals(2L, frame.degree(AGE, "30.0"));
		}

		@Test
		void degree_isNullForMissingVertex() {
			assertNull(frame.degree(PERSON, "Nobody"));
		}

		@Test
		void isOrphan_isFalseForConnectedVertex() {
			assertFalse(frame.isOrphan(PERSON, "Bob"));
		}

		@Test
		void getGraphOutput_returnsNodesAndEdges() {
			Map<? extends String, ? extends Object> output = frame.getGraphOutput();

			assertEquals(8, ((Map<?, ?>) output.get("nodes")).size());
			assertEquals(6, ((Collection<?>) output.get("edges")).size());
		}
	}

	@Nested
	class RemovingData {

		@Test
		void remove_dropsTheVerticesOfTheValues() throws Exception {
			List<Object> pets = new ArrayList<>();
			pets.add("Max");
			frame.remove(PET, pets);

			assertEquals(2, vertexCount(PET));
			assertEquals(rowsOf(new Object[] { "Joe", "Bingo", 40.0 }, new Object[] { "Ann", "Rex", 30.0 }),
					collectRows(frame.query(selectPeople())));
		}

		@Test
		void remove_ignoresMissingValues() {
			List<Object> pets = new ArrayList<>();
			pets.add("Nobody");
			frame.remove(PET, pets);

			assertEquals(3, vertexCount(PET));
		}

		@Test
		void removeColumn_dropsTheColumnAndItsVertices() throws Exception {
			frame.removeColumn(AGE);

			assertEquals(0, vertexCount(AGE));
			assertFalse(Arrays.asList(frame.getColumnHeaders()).contains(AGE));

			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(PERSON));
			qs.addSelector(new QueryColumnSelector(PET));
			qs.addRelation(PERSON, PET, "inner.join");
			assertEquals(rowsOf(new Object[] { "Bob", "Max" }, new Object[] { "Joe", "Bingo" },
					new Object[] { "Ann", "Rex" }), collectRows(frame.query(qs)));
		}

		@Test
		void removeColumn_ignoresUnknownColumn() {
			frame.removeColumn("Missing");

			assertEquals(8, vertexCount());
			assertEquals(3, frame.getColumnHeaders().length);
		}
	}

	@Nested
	class ChangingTypes {

		@Test
		void changeDataType_toStringConvertsValues() {
			frame.changeDataType(AGE, "STRING");

			Object age = frame.g.traversal().V().has(TinkerFrame.TINKER_TYPE, AGE).has(TinkerFrame.TINKER_NAME, "30.0")
					.next().value(TinkerFrame.TINKER_NAME);
			assertEquals("30.0", age);
		}

		@Test
		void changeDataType_toNumberParsesNumericText() {
			frame.changeDataType(AGE, "STRING");
			frame.changeDataType(AGE, "NUMBER");

			assertEquals(1, frame.g.traversal().V().has(TinkerFrame.TINKER_TYPE, AGE).has(TinkerFrame.TINKER_NAME, 40.0)
					.count().next());
		}

		@Test
		void changeDataType_toDateIsRejected() {
			assertThrows(IllegalArgumentException.class, () -> frame.changeDataType(AGE, "DATE"));
		}
	}

	@Nested
	class Caching {

		@Test
		void saveAndOpen_restoresTheGraph(@TempDir Path folder) throws Exception {
			assumeTrue(Object.class.getModule().isOpen("java.util.concurrent.atomic", TinkerFrame.class.getModule()),
					"Gryo serialization needs --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED");
			CachePropFileFrameObject cf = frame.save(folder.toString(), null);

			assertTrue(new File(cf.getFrameCacheLocation()).isFile());
			assertTrue(new File(cf.getFrameMetaCacheLocation()).isFile());
			assertEquals(TinkerFrame.class.getName(), cf.getFrameType());

			TinkerFrame reopened = new TinkerFrame();
			try {
				reopened.open(cf, null);

				assertEquals(FRAME_NAME, reopened.getName());
				assertEquals(8, reopened.g.traversal().V().count().next());
				assertEquals(rowsOf(new Object[] { "Bob", "Max", 30.0 }, new Object[] { "Joe", "Bingo", 40.0 },
						new Object[] { "Ann", "Rex", 30.0 }), collectRows(reopened.query(selectPeople())));
			} finally {
				reopened.close();
			}
		}
	}

	@Nested
	class Closing {

		@Test
		void close_marksTheFrameClosed() {
			frame.close();

			assertTrue(frame.isClosed());
		}
	}
}
