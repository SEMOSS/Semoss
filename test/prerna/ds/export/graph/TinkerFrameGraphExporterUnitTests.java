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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.Color;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.ds.TinkerFrame;

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

}
