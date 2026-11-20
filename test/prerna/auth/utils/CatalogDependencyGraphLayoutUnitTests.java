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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.CatalogDependencyGraphLayout.Direction;
import prerna.auth.utils.CatalogDependencyGraphLayout.LayoutEdge;
import prerna.auth.utils.CatalogDependencyGraphLayout.LayoutNode;
import prerna.auth.utils.CatalogDependencyGraphLayout.Position;
import prerna.auth.utils.CatalogDependencyGraphLayout.Result;

/** The admin dependency map's layout, cycle detection and tracing, without a database. */
public class CatalogDependencyGraphLayoutUnitTests {

	private static final double W = CatalogDependencyGraphLayout.NODE_WIDTH;
	private static final double H = CatalogDependencyGraphLayout.NODE_HEIGHT;

	private static LayoutNode node(String id) {
		return new LayoutNode(id, "ENGINE:MODEL", id);
	}

	private static LayoutNode node(String id, String type, String name) {
		return new LayoutNode(id, type, name);
	}

	private static LayoutEdge edge(String source, String target) {
		return new LayoutEdge(source + "|" + target + "|MANUAL", source, target);
	}

	/** Two components (a chain of 5 and a star of 4), a 2 node cycle, a self loop, and isolated nodes */
	private static List<LayoutNode> sampleNodes() {
		List<LayoutNode> nodes = new ArrayList<>();
		for (String id : List.of("a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3", "b4", "c1", "c2", "d1")) {
			nodes.add(node(id));
		}
		nodes.add(node("i1", "PROJECT:SKILL", "Zeta"));
		nodes.add(node("i2", "ENGINE:DATABASE", "Beta"));
		nodes.add(node("i3", "ENGINE:DATABASE", "Alpha"));
		nodes.add(node("i4", "PROJECT:SKILL", "Eta"));
		return nodes;
	}

	private static List<LayoutEdge> sampleEdges() {
		return List.of(edge("a1", "a2"), edge("a2", "a3"), edge("a3", "a4"), edge("a4", "a5"), edge("b1", "b2"),
				edge("b1", "b3"), edge("b1", "b4"), edge("c1", "c2"), edge("c2", "c1"), edge("d1", "d1"));
	}

	private static boolean overlaps(Position a, Position b) {
		return a.x() < b.x() + W && b.x() < a.x() + W && a.y() < b.y() + H && b.y() < a.y() + H;
	}

	private static void assertNoOverlaps(Map<String, Position> positions) {
		List<Map.Entry<String, Position>> entries = new ArrayList<>(positions.entrySet());
		for (int i = 0; i < entries.size(); i++) {
			for (int j = i + 1; j < entries.size(); j++) {
				assertFalse(overlaps(entries.get(i).getValue(), entries.get(j).getValue()),
						entries.get(i).getKey() + " overlaps " + entries.get(j).getKey());
			}
		}
	}

	/** The bounding box of each component's boxes, as minX, minY, maxX, maxY */
	private static Map<Integer, double[]> componentBoxes(Result result) {
		Map<Integer, double[]> boxes = new LinkedHashMap<>();
		for (Map.Entry<String, Position> entry : result.positions().entrySet()) {
			int component = result.components().get(entry.getKey());
			Position p = entry.getValue();
			double[] box = boxes.computeIfAbsent(component, key -> new double[] { Double.MAX_VALUE,
					Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE });
			box[0] = Math.min(box[0], p.x());
			box[1] = Math.min(box[1], p.y());
			box[2] = Math.max(box[2], p.x() + W);
			box[3] = Math.max(box[3], p.y() + H);
		}
		return boxes;
	}

	@Test
	void sameInputInAnyOrderGivesTheSameLayout() {
		Result first = CatalogDependencyGraphLayout.layout(sampleNodes(), sampleEdges());
		List<LayoutNode> shuffledNodes = new ArrayList<>(sampleNodes());
		List<LayoutEdge> shuffledEdges = new ArrayList<>(sampleEdges());
		Collections.reverse(shuffledNodes);
		Collections.reverse(shuffledEdges);
		Result second = CatalogDependencyGraphLayout.layout(shuffledNodes, shuffledEdges);

		assertEquals(first.positions(), second.positions());
		assertEquals(first.components(), second.components());
		assertEquals(first.inCycleEdges(), second.inCycleEdges());
		assertEquals(first.width(), second.width());
		assertEquals(first.height(), second.height());
	}

	@Test
	void everyNodeIsPlacedWithoutOverlapsAndNormalizedToZero() {
		Result result = CatalogDependencyGraphLayout.layout(sampleNodes(), sampleEdges());
		assertEquals(16, result.positions().size());
		assertNoOverlaps(result.positions());
		double minX = result.positions().values().stream().mapToDouble(Position::x).min().orElseThrow();
		double minY = result.positions().values().stream().mapToDouble(Position::y).min().orElseThrow();
		assertEquals(0, minX);
		assertEquals(0, minY);
		for (Position p : result.positions().values()) {
			assertTrue(p.x() + W <= result.width());
			assertTrue(p.y() + H <= result.height());
		}
	}

	@Test
	void componentsAreNumberedLargestFirstAndDoNotOverlap() {
		Result result = CatalogDependencyGraphLayout.layout(sampleNodes(), sampleEdges());
		// a chain, b star, c cycle, d self loop
		assertEquals(4, result.componentCount());
		assertEquals(0, result.components().get("a1"));
		assertEquals(1, result.components().get("b1"));
		assertEquals(2, result.components().get("c1"));
		assertEquals(3, result.components().get("d1"));
		for (String id : List.of("a2", "a3", "a4", "a5")) {
			assertEquals(0, result.components().get(id));
		}

		Map<Integer, double[]> boxes = componentBoxes(result);
		boxes.remove(-1);
		List<double[]> list = new ArrayList<>(boxes.values());
		for (int i = 0; i < list.size(); i++) {
			for (int j = i + 1; j < list.size(); j++) {
				double[] a = list.get(i);
				double[] b = list.get(j);
				boolean apart = a[2] <= b[0] || b[2] <= a[0] || a[3] <= b[1] || b[3] <= a[1];
				assertTrue(apart, "components " + i + " and " + j + " overlap");
			}
		}
	}

	@Test
	void isolatedNodesComeLastGroupedByTypeThenName() {
		Result result = CatalogDependencyGraphLayout.layout(sampleNodes(), sampleEdges());
		assertEquals(4, result.isolatedCount());
		for (String id : List.of("i1", "i2", "i3", "i4")) {
			assertEquals(-1, result.components().get(id));
		}
		double lowestLinked = 0;
		for (Map.Entry<String, Position> entry : result.positions().entrySet()) {
			if (result.components().get(entry.getKey()) >= 0) {
				lowestLinked = Math.max(lowestLinked, entry.getValue().y() + H);
			}
		}
		for (String id : List.of("i1", "i2", "i3", "i4")) {
			assertTrue(result.positions().get(id).y() >= lowestLinked, id + " is not below the components");
		}
		// ENGINE:DATABASE before PROJECT:SKILL, each by name, each type on its own row
		Position alpha = result.positions().get("i3");
		Position beta = result.positions().get("i2");
		Position eta = result.positions().get("i4");
		Position zeta = result.positions().get("i1");
		assertEquals(alpha.y(), beta.y());
		assertTrue(alpha.x() < beta.x());
		assertEquals(eta.y(), zeta.y());
		assertTrue(eta.x() < zeta.x());
		assertTrue(eta.y() > alpha.y());
	}

	@Test
	void onlyIsolatedNodesFormAGridFromZero() {
		List<LayoutNode> nodes = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			nodes.add(node(String.format("n%02d", i), "ENGINE:MODEL", String.format("n%02d", i)));
		}
		Result result = CatalogDependencyGraphLayout.layout(nodes, List.of());
		assertEquals(30, result.isolatedCount());
		assertEquals(0, result.componentCount());
		assertNoOverlaps(result.positions());
		assertEquals(new Position(0, 0), result.positions().get("n00"));
		// wider than tall
		assertTrue(result.width() > result.height());
	}

	@Test
	void twoNodeCyclesAndSelfLoopsAreFlagged() {
		Result result = CatalogDependencyGraphLayout.layout(sampleNodes(), sampleEdges());
		assertEquals(Set.of("c1|c2|MANUAL", "c2|c1|MANUAL", "d1|d1|MANUAL"), result.inCycleEdges());
		assertEquals(2, result.cycles());
	}

	@Test
	void aLongerCycleFlagsOnlyItsOwnLinks() {
		List<LayoutNode> nodes = List.of(node("x"), node("y"), node("z"), node("tail"));
		List<LayoutEdge> edges = List.of(edge("x", "y"), edge("y", "z"), edge("z", "x"), edge("z", "tail"));
		Result result = CatalogDependencyGraphLayout.layout(nodes, edges);
		assertEquals(Set.of("x|y|MANUAL", "y|z|MANUAL", "z|x|MANUAL"), result.inCycleEdges());
		assertEquals(1, result.cycles());
	}

	@Test
	void linksToUnknownNodesAreLeftOut() {
		Result result = CatalogDependencyGraphLayout.layout(List.of(node("a")), List.of(edge("a", "ghost")));
		assertEquals(Set.of("a"), result.positions().keySet());
		assertEquals(1, result.isolatedCount());
	}

	@Test
	void aComponentAboveTheThresholdIsSnappedToAGrid() {
		List<LayoutNode> nodes = new ArrayList<>();
		List<LayoutEdge> edges = new ArrayList<>();
		for (int i = 0; i < 40; i++) {
			nodes.add(node(String.format("g%02d", i)));
			if (i > 0) {
				edges.add(edge(String.format("g%02d", i), String.format("g%02d", (i - 1) / 3)));
			}
		}
		Result grid = CatalogDependencyGraphLayout.layout(nodes, edges, 10);
		assertNoOverlaps(grid.positions());
		double cellWidth = W + CatalogDependencyGraphLayout.NODE_GAP;
		double cellHeight = H + CatalogDependencyGraphLayout.NODE_GAP;
		for (Position p : grid.positions().values()) {
			assertEquals(0, Math.IEEEremainder(p.x(), cellWidth), 1e-6);
			assertEquals(0, Math.IEEEremainder(p.y(), cellHeight), 1e-6);
		}
		assertEquals(grid.positions(), CatalogDependencyGraphLayout.layout(nodes, edges, 10).positions());

		// below the threshold the same graph is separated with Rectangles instead
		Result separated = CatalogDependencyGraphLayout.layout(nodes, edges, 1_000);
		assertNoOverlaps(separated.positions());
	}

	@Test
	void snapToGridMovesCollisionsToTheNearestFreeCell() {
		Map<String, double[]> scaled = new LinkedHashMap<>();
		scaled.put("a", new double[] { 0, 0 });
		scaled.put("b", new double[] { 1, 1 });
		scaled.put("c", new double[] { 2, 2 });
		Map<String, Position> placed = CatalogDependencyGraphLayout.snapToGrid(scaled);
		assertEquals(new Position(0, 0), placed.get("a"));
		assertEquals(3, new HashSet<>(placed.values()).size());
		assertNoOverlaps(placed);
	}

	@Test
	void seedDependsOnlyOnTheIds() {
		assertEquals(CatalogDependencyGraphLayout.seedOf(List.of("a", "b")),
				CatalogDependencyGraphLayout.seedOf(List.of("a", "b")));
		assertFalse(CatalogDependencyGraphLayout.seedOf(List.of("a", "b")) == CatalogDependencyGraphLayout
				.seedOf(List.of("ab")));
	}

	///
	/// trace
	///

	/** root depends on d1, d1 on d2; u1 depends on root, u2 on u1; u1 also depends on side */
	private static List<LayoutEdge> traceEdges() {
		return List.of(edge("root", "d1"), edge("d1", "d2"), edge("u1", "root"), edge("u2", "u1"),
				edge("u1", "side"), edge("other", "unrelated"));
	}

	@Test
	void traceFollowsDependencies() {
		assertEquals(Set.of("root", "d1", "d2"),
				CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.DEPENDENCIES, null));
		assertEquals(Set.of("root", "d1"),
				CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.DEPENDENCIES, 1));
		assertEquals(Set.of("root"),
				CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.DEPENDENCIES, 0));
	}

	@Test
	void traceFollowsDependents() {
		assertEquals(Set.of("root", "u1", "u2"),
				CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.DEPENDENTS, null));
		assertEquals(Set.of("root", "u1"),
				CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.DEPENDENTS, 1));
	}

	@Test
	void traceBothFollowsEachDirectionOnItsOwn() {
		// side is a dependency of a dependent, so it is not reached
		assertEquals(Set.of("root", "d1", "d2", "u1", "u2"),
				CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.BOTH, null));
		assertEquals(Set.of("root", "d1", "u1"),
				CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.BOTH, 1));
	}

	@Test
	void traceHandlesCyclesAndKeepsTheLinksAmongTheNodes() {
		List<LayoutEdge> edges = List.of(edge("a", "b"), edge("b", "a"), edge("b", "c"));
		Set<String> reached = CatalogDependencyGraphLayout.trace(edges, "a", Direction.DEPENDENCIES, null);
		assertEquals(Set.of("a", "b", "c"), reached);
		assertEquals(3, CatalogDependencyGraphLayout.edgesAmong(edges, reached).size());
		assertEquals(List.of(edge("a", "b"), edge("b", "a")),
				CatalogDependencyGraphLayout.edgesAmong(edges, Set.of("a", "b")));
	}

	@Test
	void directionParsing() {
		assertEquals(Direction.BOTH, Direction.parse(null));
		assertEquals(Direction.DEPENDENTS, Direction.parse(" dependents "));
		assertThrows(IllegalArgumentException.class, () -> Direction.parse("UP"));
		assertThrows(IllegalArgumentException.class,
				() -> CatalogDependencyGraphLayout.trace(traceEdges(), "root", Direction.BOTH, -1));
	}
}
