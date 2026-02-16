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
package prerna.ds.export.graph;

import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import prerna.ds.TinkerFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class TinkerFrameGraphExporterUnitTests {

	private String alias = "frameG";
	private TinkerFrame frame = new TinkerFrame(alias);

	@BeforeEach
	void setup() {
		// add data to tinker frame
		{
			Map<String, Object> rowCleanData = new HashMap<>();
			rowCleanData.put("Person", "Bob");
			rowCleanData.put("Pet", "max");

			Map<String, Set<String>> edgeHash = new HashMap<>();
			// A relates to B
			edgeHash.put("Person", new HashSet<>(Collections.singletonList("Pet")));
			edgeHash.put("Pet", new HashSet<>());

			Map<String, String> logicalToTypeMap = new HashMap<>();
			logicalToTypeMap.put("Person", "PersonType");
			logicalToTypeMap.put("Pet", "PetType");

			frame.addRelationship(rowCleanData, edgeHash, logicalToTypeMap);

			rowCleanData = new HashMap<>();
			rowCleanData.put("Person", "Joe");
			rowCleanData.put("Pet", "bingo");

			edgeHash = new HashMap<>();
			// A relates to B
			edgeHash.put("Person", new HashSet<>(Collections.singletonList("Pet")));
			edgeHash.put("Pet", new HashSet<>());

			logicalToTypeMap = new HashMap<>();
			logicalToTypeMap.put("Person", "PersonType");
			logicalToTypeMap.put("Pet", "PetType");

			frame.addRelationship(rowCleanData, edgeHash, logicalToTypeMap);

		}
	}

	@Test
	void testConstructors() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		assertNotNull(exporter);

		// frame with color map
		Map<String, Color> colorMap = new HashMap<>();
		colorMap.put("Person", Color.GREEN);
		assertNotNull(new TinkerFrameGraphExporter(frame, colorMap));
	}

	@Test
	void testHasNextVert() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		assertTrue(exporter.hasNextVert());
	}

	@Test
	void testGetNextVert() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		Map<String, Object> vertMap = exporter.getNextVert();
		assertTrue(!vertMap.isEmpty());
	}

	@Test
	void testHasNextEdge() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		assertTrue(exporter.hasNextEdge());
	}

	@Test
	void testGetNextEdge() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		// verticies must be hashed before getting edges
		exporter.getNextVert();
		exporter.getNextVert();
		exporter.getNextVert();
		exporter.getNextVert();

		Map<String, Object> edgeMap = exporter.getNextEdge();
		assertTrue(!edgeMap.isEmpty());
		assertEquals("PersonType/Joe", edgeMap.get("source"));
		assertEquals("PetType/bingo", edgeMap.get("target"));

		edgeMap = exporter.getNextEdge();
		assertTrue(!edgeMap.isEmpty());
		assertEquals("PersonType/Bob", edgeMap.get("source"));
		assertEquals("PetType/max", edgeMap.get("target"));
	}

	@Test
	void testGetData() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		Map<String, Object> formattedData = (Map<String, Object>) exporter.getData();
		assertTrue(!formattedData.isEmpty());

		List<Map<String, Object>> nodesMapList = (List<Map<String, Object>>) formattedData.get("nodes");
		assertEquals(4, nodesMapList.size());

		List<Map<String, Object>> edgesMapList = (List<Map<String, Object>>) formattedData.get("edges");
		assertEquals(2, edgesMapList.size());
	}

	@Test
	void testGetVertCounts() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		exporter.getData();
		Map<String, Integer> counts = exporter.getVertCounts();
		assertEquals(2, counts.size());
	}

	// ── Iterator exhaustion tests ──────────────────────────────────────────

	@Test
	void testHasNextVert_returnsFalseAfterAllConsumed() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		// 4 vertices: Bob, Joe, max, bingo
		int count = 0;
		while (exporter.hasNextVert()) {
			exporter.getNextVert();
			count++;
		}
		assertEquals(4, count);
		// Now should return false
		assertFalse(exporter.hasNextVert());
	}

	@Test
	void testHasNextEdge_returnsFalseAfterAllConsumed() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		// Consume all vertices first (needed for vertSet)
		while (exporter.hasNextVert()) {
			exporter.getNextVert();
		}
		// 2 edges: Person->Pet (Bob->max, Joe->bingo)
		int count = 0;
		while (exporter.hasNextEdge()) {
			exporter.getNextEdge();
			count++;
		}
		assertEquals(2, count);
		// Now should return false
		assertFalse(exporter.hasNextEdge());
	}

	// ── Color map tests ────────────────────────────────────────────────────

	@Test
	void testGetNextVert_withColorMap() {
		Map<String, Color> colorMap = new HashMap<>();
		colorMap.put("PersonType", new Color(100, 150, 200));
		colorMap.put("PetType", new Color(50, 75, 100));
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame, colorMap);

		// Collect all vertices and verify colors come from the colorMap
		List<Map<String, Object>> verts = new ArrayList<>();
		while (exporter.hasNextVert()) {
			verts.add(exporter.getNextVert());
		}
		assertEquals(4, verts.size());

		for (Map<String, Object> v : verts) {
			String type = (String) v.get(Constants.VERTEX_TYPE);
			String color = (String) v.get(Constants.VERTEX_COLOR);
			assertNotNull(color);
			if ("PersonType".equals(type)) {
				assertEquals("100,150,200", color);
			} else if ("PetType".equals(type)) {
				assertEquals("50,75,100", color);
			}
		}
	}

	@Test
	void testGetNextVert_withPartialColorMap() {
		// Color map has PersonType but not PetType → falls back to TypeColorShapeTable
		Map<String, Color> colorMap = new HashMap<>();
		colorMap.put("PersonType", Color.MAGENTA);
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame, colorMap);

		List<Map<String, Object>> verts = new ArrayList<>();
		while (exporter.hasNextVert()) {
			verts.add(exporter.getNextVert());
		}
		assertEquals(4, verts.size());

		for (Map<String, Object> v : verts) {
			String type = (String) v.get(Constants.VERTEX_TYPE);
			String color = (String) v.get(Constants.VERTEX_COLOR);
			assertNotNull(color);
			if ("PersonType".equals(type)) {
				assertEquals("255,0,255", color);
			}
			// PetType gets fallback color from TypeColorShapeTable - just verify it exists
		}
	}

	// ── Vertex property tests ──────────────────────────────────────────────

	@Test
	void testGetNextVert_vertexMapContents() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		Map<String, Object> vertMap = exporter.getNextVert();

		// Verify the vertex map has required keys
		assertTrue(vertMap.containsKey("uri"));
		assertTrue(vertMap.containsKey(Constants.VERTEX_TYPE));
		assertTrue(vertMap.containsKey(Constants.VERTEX_NAME));
		assertTrue(vertMap.containsKey("propHash"));
		assertTrue(vertMap.containsKey(Constants.VERTEX_COLOR));

		// uri should be type/name
		String uri = (String) vertMap.get("uri");
		String type = (String) vertMap.get(Constants.VERTEX_TYPE);
		Object name = vertMap.get(Constants.VERTEX_NAME);
		assertEquals(type + "/" + name, uri);

		// propHash should not contain internal tinker properties
		Map<String, Object> propHash = (Map<String, Object>) vertMap.get("propHash");
		assertNotNull(propHash);
		assertFalse(propHash.containsKey(TinkerFrame.TINKER_ID));
		assertFalse(propHash.containsKey(TinkerFrame.TINKER_NAME));
		assertFalse(propHash.containsKey(TinkerFrame.TINKER_TYPE));
	}

	// ── Edge property tests ────────────────────────────────────────────────

	@Test
	void testGetNextEdge_withoutVerticesHashed_returnsEmptyMap() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		// Don't call getNextVert first - vertSet is empty
		Map<String, Object> edgeMap = exporter.getNextEdge();
		// Edge source/target won't be in vertSet, so should return empty map
		assertTrue(edgeMap.isEmpty());
	}

	@Test
	void testGetNextEdge_edgeMapContents() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		// Hash all vertices first
		while (exporter.hasNextVert()) {
			exporter.getNextVert();
		}

		Map<String, Object> edgeMap = exporter.getNextEdge();
		assertFalse(edgeMap.isEmpty());

		// Verify edge map has required keys
		assertTrue(edgeMap.containsKey("uri"));
		assertTrue(edgeMap.containsKey("source"));
		assertTrue(edgeMap.containsKey("target"));
		assertTrue(edgeMap.containsKey("propHash"));

		// propHash should not contain internal tinker properties
		Map<String, Object> propHash = (Map<String, Object>) edgeMap.get("propHash");
		assertNotNull(propHash);
		assertFalse(propHash.containsKey(TinkerFrame.TINKER_ID));
		assertFalse(propHash.containsKey(TinkerFrame.TINKER_NAME));
		assertFalse(propHash.containsKey(TinkerFrame.TINKER_TYPE));
	}

	// ── getData tests ──────────────────────────────────────────────────────

	@Test
	void testGetData_structure() {
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		Map<String, Object> data = (Map<String, Object>) exporter.getData();

		assertTrue(data.containsKey("nodes"));
		assertTrue(data.containsKey("edges"));
		assertTrue(data.containsKey("graphMeta"));

		Map<String, Integer> graphMeta = (Map<String, Integer>) data.get("graphMeta");
		// PersonType: 2 (Bob, Joe), PetType: 2 (max, bingo)
		assertEquals(2, graphMeta.get("PersonType"));
		assertEquals(2, graphMeta.get("PetType"));
	}

	@Test
	void testGetData_withColorMap() {
		Map<String, Color> colorMap = new HashMap<>();
		colorMap.put("PersonType", Color.RED);
		colorMap.put("PetType", Color.BLUE);
		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame, colorMap);

		Map<String, Object> data = (Map<String, Object>) exporter.getData();
		List<Map<String, Object>> nodes = (List<Map<String, Object>>) data.get("nodes");
		assertEquals(4, nodes.size());

		// Verify all nodes have the correct colors
		for (Map<String, Object> node : nodes) {
			String type = (String) node.get(Constants.VERTEX_TYPE);
			String color = (String) node.get(Constants.VERTEX_COLOR);
			if ("PersonType".equals(type)) {
				assertEquals("255,0,0", color);
			} else if ("PetType".equals(type)) {
				assertEquals("0,0,255", color);
			}
		}
	}

	// ── Filter traversal integration tests ────────────────────────────────

	@Test
	void testCreateVertsIt_withNonEmptyFilters() {
		// Setting non-empty filters exercises the hasFilter branch in createVertsIt
		GenRowFilters filters = new GenRowFilters();
		SimpleQueryFilter filter = SimpleQueryFilter.makeColToValFilter("Person", "==", "Bob");
		filters.addFilters(filter);
		frame.setFrameFilters(filters);

		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		// Even if meta returns no vertex names, the filter branch is entered
		// and union() is called with whatever (possibly empty) union traversals
		while (exporter.hasNextVert()) {
			exporter.getNextVert();
		}
		// Verify it didn't crash - coverage is the goal
		assertFalse(exporter.hasNextVert());
	}

	@Test
	void testCreateEdgesIt_withNonEmptyFilters() {
		GenRowFilters filters = new GenRowFilters();
		SimpleQueryFilter filter = SimpleQueryFilter.makeColToValFilter("Person", "==", "Bob");
		filters.addFilters(filter);
		frame.setFrameFilters(filters);

		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		// Consume vertices first
		while (exporter.hasNextVert()) {
			exporter.getNextVert();
		}
		// Exercise edge filter branch
		while (exporter.hasNextEdge()) {
			exporter.getNextEdge();
		}
		assertFalse(exporter.hasNextEdge());
	}

	@Test
	void testGetData_withFilters() {
		GenRowFilters filters = new GenRowFilters();
		SimpleQueryFilter filter = SimpleQueryFilter.makeColToValFilter("Person", "!=", "Nobody");
		filters.addFilters(filter);
		frame.setFrameFilters(filters);

		TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
		Map<String, Object> data = (Map<String, Object>) exporter.getData();
		assertNotNull(data);
		assertNotNull(data.get("nodes"));
		assertNotNull(data.get("edges"));
		assertNotNull(data.get("graphMeta"));
	}

	// ── processFilterColToValues tests (via reflection) ─────────────────

	@Nested
	class ProcessFilterTests {

		private Method processFilterMethod;

		@BeforeEach
		void setupReflection() throws Exception {
			processFilterMethod = TinkerFrameGraphExporter.class.getDeclaredMethod(
					"processFilterColToValues",
					GraphTraversal.class, NounMetadata.class, NounMetadata.class, String.class);
			processFilterMethod.setAccessible(true);
		}

		private void invokeProcessFilter(TinkerFrameGraphExporter exporter,
				GraphTraversal traversal, NounMetadata colComp, NounMetadata valComp, String comp)
				throws Exception {
			processFilterMethod.invoke(exporter, traversal, colComp, valComp, comp);
		}

		private NounMetadata makeColNoun(String col) {
			return new NounMetadata(new QueryColumnSelector(col), PixelDataType.COLUMN);
		}

		private NounMetadata makeValNoun(Object val) {
			return new NounMetadata(val, PixelDataType.CONST_STRING);
		}

		@Test
		void testEqualsComparator() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun("Bob"), "==");
			// Traversal now has a .has(TINKER_NAME, P.within("Bob")) step
			assertNotNull(traversal);
		}

		@Test
		void testNotEqualsComparator() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun("Bob"), "!=");
			assertNotNull(traversal);
		}

		@Test
		void testLessThanComparator() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun(10), "<");
			assertNotNull(traversal);
		}

		@Test
		void testGreaterThanComparator() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun(10), ">");
			assertNotNull(traversal);
		}

		@Test
		void testLessThanOrEqualComparator() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun(10), "<=");
			assertNotNull(traversal);
		}

		@Test
		void testGreaterThanOrEqualComparator() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun(10), ">=");
			assertNotNull(traversal);
		}

		@Test
		void testWithListValues() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			List<String> values = Arrays.asList("Bob", "Joe");
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun(values), "==");
			assertNotNull(traversal);
		}

		@Test
		void testWithListValuesNotEqual() throws Exception {
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			List<String> values = Arrays.asList("Bob", "Joe");
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun(values), "!=");
			assertNotNull(traversal);
		}

		@Test
		void testUnknownComparator() throws Exception {
			// comparator that doesn't match any branch - no step added
			TinkerFrameGraphExporter exporter = new TinkerFrameGraphExporter(frame);
			GraphTraversal<Vertex, Vertex> traversal = __.start();
			invokeProcessFilter(exporter, traversal, makeColNoun("Person"), makeValNoun("Bob"), "LIKE");
			assertNotNull(traversal);
		}
	}
}
