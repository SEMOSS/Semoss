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

import java.awt.geom.Rectangle2D;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.graphstream.algorithm.ConnectedComponents;
import org.graphstream.algorithm.ConnectedComponents.ConnectedComponent;
import org.graphstream.algorithm.TarjanStronglyConnectedComponents;
import org.graphstream.algorithm.Toolkit;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.ui.layout.Layout;
import org.graphstream.ui.layout.springbox.implementations.SpringBox;

import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.reactor.masterdatabase.util.Rectangles;

/**
 * Lays out the catalog dependency graph for the admin dependency map: a position
 * in pixels for every engine and project, which weakly connected component each
 * belongs to, and which links are part of a cycle. Pure: it reads no database,
 * and the same nodes and links always give the same result, whatever order they
 * come in, as long as the force layout settles within its step and time limits.
 *
 * <p>
 * Each component of two or more nodes is laid out on its own with a seeded
 * SpringBox, scaled to pixels and separated so no two boxes overlap. The
 * components are then shelf packed, largest first, to roughly a 16:9 area, and
 * the nodes without any link follow below as one block, grouped by type.
 */
public final class CatalogDependencyGraphLayout {

	/** The width of a node's box, in pixels */
	public static final double NODE_WIDTH = 240;
	/** The height of a node's box, in pixels */
	public static final double NODE_HEIGHT = 64;
	/** The space kept between the boxes of one component */
	static final double NODE_GAP = 40;
	/** The space kept between two components, and before the isolated block */
	static final double COMPONENT_GAP = 200;
	/** Pixels per SpringBox unit across */
	static final double SCALE_X = 320;
	/** Pixels per SpringBox unit down */
	static final double SCALE_Y = 160;
	/** The width to height ratio the packing aims for */
	static final double ASPECT = 16.0 / 9.0;
	/**
	 * Above this many nodes a component is snapped to a grid rather than separated
	 * with {@link Rectangles}, which compares every pair
	 */
	public static final int DEFAULT_GRID_THRESHOLD = 2_500;

	private static final double CELL_WIDTH = NODE_WIDTH + NODE_GAP;
	private static final double CELL_HEIGHT = NODE_HEIGHT + NODE_GAP;

	private CatalogDependencyGraphLayout() {

	}

	/** A node to place: its id, its type to group isolated nodes by, and its name */
	public record LayoutNode(String id, String type, String name) {
	}

	/** A link from the item that depends (source) to what it depends on (target) */
	public record LayoutEdge(String id, String source, String target) {
	}

	/** A node's top left corner, in pixels */
	public record Position(double x, double y) {
	}

	/** Which way to follow the links from a node */
	public enum Direction {
		/** What it depends on, and what those depend on */
		DEPENDENCIES,
		/** What depends on it, and what depends on those */
		DEPENDENTS,
		/** Both */
		BOTH;

		/**
		 * @param value the direction, in any case; null or blank for BOTH
		 * @return the direction
		 * @throws IllegalArgumentException when it is none of them
		 */
		public static Direction parse(String value) {
			if (value == null || value.isBlank()) {
				return BOTH;
			}
			try {
				return valueOf(value.trim().toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(
						"Unknown direction '" + value + "'. Use DEPENDENCIES, DEPENDENTS or BOTH");
			}
		}
	}

	/** The laid out graph */
	public static final class Result {
		private final Map<String, Position> positions;
		private final Map<String, Integer> components;
		private final Set<String> inCycleEdges;
		private final int cycles;
		private final int componentCount;
		private final int isolatedCount;
		private final double width;
		private final double height;

		Result(Map<String, Position> positions, Map<String, Integer> components, Set<String> inCycleEdges,
				int cycles, int componentCount, int isolatedCount, double width, double height) {
			this.positions = Collections.unmodifiableMap(positions);
			this.components = Collections.unmodifiableMap(components);
			this.inCycleEdges = Collections.unmodifiableSet(inCycleEdges);
			this.cycles = cycles;
			this.componentCount = componentCount;
			this.isolatedCount = isolatedCount;
			this.width = width;
			this.height = height;
		}

		/** Each node's top left corner, by id */
		public Map<String, Position> positions() {
			return positions;
		}

		/**
		 * Each node's component, by id: 0 for the largest, in the order they are
		 * packed, and -1 for a node without any link
		 */
		public Map<String, Integer> components() {
			return components;
		}

		/** The ids of the links that are part of a cycle, a link to itself included */
		public Set<String> inCycleEdges() {
			return inCycleEdges;
		}

		/**
		 * How many cycles there are: each set of two or more nodes that all reach
		 * each other, and each node that links to itself outside such a set
		 */
		public int cycles() {
			return cycles;
		}

		/** How many components have at least one link */
		public int componentCount() {
			return componentCount;
		}

		/** How many nodes have no link */
		public int isolatedCount() {
			return isolatedCount;
		}

		/** The width of the whole layout, in pixels */
		public double width() {
			return width;
		}

		/** The height of the whole layout, in pixels */
		public double height() {
			return height;
		}
	}

	/**
	 * Lays out a graph, snapping components above
	 * {@value #DEFAULT_GRID_THRESHOLD} nodes to a grid
	 *
	 * @param nodes the nodes; a repeated id is kept once
	 * @param edges the links; a repeated id is kept once, and one whose ends are
	 *              not both nodes is left out
	 * @return the layout
	 */
	public static Result layout(Collection<LayoutNode> nodes, Collection<LayoutEdge> edges) {
		return layout(nodes, edges, DEFAULT_GRID_THRESHOLD);
	}

	/**
	 * Lays out a graph
	 *
	 * @param nodes         the nodes; a repeated id is kept once
	 * @param edges         the links; a repeated id is kept once, and one whose
	 *                      ends are not both nodes is left out
	 * @param gridThreshold above this many nodes a component is snapped to a grid
	 * @return the layout
	 */
	static Result layout(Collection<LayoutNode> nodes, Collection<LayoutEdge> edges, int gridThreshold) {
		// sorted so the graph is built the same way whatever order they came in
		TreeMap<String, LayoutNode> byId = new TreeMap<>();
		for (LayoutNode node : nodes) {
			byId.putIfAbsent(node.id(), node);
		}
		TreeMap<String, LayoutEdge> edgesById = new TreeMap<>();
		for (LayoutEdge edge : edges) {
			if (byId.containsKey(edge.source()) && byId.containsKey(edge.target())) {
				edgesById.putIfAbsent(edge.id(), edge);
			}
		}

		Graph graph = new MultiGraph("catalog-dependencies");
		for (String id : byId.keySet()) {
			graph.addNode(id);
		}
		for (LayoutEdge edge : edgesById.values()) {
			graph.addEdge(edge.id(), edge.source(), edge.target(), true);
		}

		// cycles
		TarjanStronglyConnectedComponents tarjan = new TarjanStronglyConnectedComponents();
		tarjan.init(graph);
		tarjan.compute();
		Map<String, Object> sccOf = new HashMap<>();
		Map<Object, Integer> sccSize = new HashMap<>();
		for (String id : byId.keySet()) {
			Object scc = graph.getNode(id).getAttribute(tarjan.getSCCIndexAttribute());
			sccOf.put(id, scc);
			sccSize.merge(scc, 1, Integer::sum);
		}
		Set<String> inCycle = new LinkedHashSet<>();
		Set<Object> cyclicSccs = new HashSet<>();
		Set<String> selfLoopNodes = new HashSet<>();
		for (LayoutEdge edge : edgesById.values()) {
			if (edge.source().equals(edge.target())) {
				inCycle.add(edge.id());
				selfLoopNodes.add(edge.source());
				continue;
			}
			Object scc = sccOf.get(edge.source());
			if (scc != null && scc.equals(sccOf.get(edge.target())) && sccSize.get(scc) > 1) {
				inCycle.add(edge.id());
				cyclicSccs.add(scc);
			}
		}
		int cycles = cyclicSccs.size();
		for (String id : selfLoopNodes) {
			if (sccSize.get(sccOf.get(id)) <= 1) {
				cycles++;
			}
		}

		// components, by the links in either direction
		Set<String> linked = new HashSet<>();
		for (LayoutEdge edge : edgesById.values()) {
			linked.add(edge.source());
			linked.add(edge.target());
		}
		ConnectedComponents connected = new ConnectedComponents(graph);
		connected.compute();
		List<List<String>> components = new ArrayList<>();
		for (ConnectedComponent component : connected) {
			List<String> ids = new ArrayList<>();
			for (Node node : component.getNodeSet()) {
				ids.add(node.getId());
			}
			if (ids.size() == 1 && !linked.contains(ids.get(0))) {
				continue;
			}
			Collections.sort(ids);
			components.add(ids);
		}
		connected.terminate();
		components.sort(Comparator.comparingInt((List<String> ids) -> -ids.size()).thenComparing(ids -> ids.get(0)));

		List<Map<String, Position>> placedComponents = new ArrayList<>();
		for (List<String> ids : components) {
			placedComponents.add(layoutComponent(ids, edgesById.values(), gridThreshold));
		}

		Map<String, Position> positions = new LinkedHashMap<>();
		Map<String, Integer> componentOf = new LinkedHashMap<>();

		// shelf pack the components, largest first
		double totalArea = 0;
		double widest = 0;
		List<double[]> sizes = new ArrayList<>();
		for (Map<String, Position> placed : placedComponents) {
			double[] size = extent(placed);
			sizes.add(size);
			totalArea += (size[0] + COMPONENT_GAP) * (size[1] + COMPONENT_GAP);
			widest = Math.max(widest, size[0]);
		}
		double targetWidth = Math.max(widest, Math.sqrt(totalArea * ASPECT));
		double x = 0;
		double y = 0;
		double shelfHeight = 0;
		double packedWidth = 0;
		for (int i = 0; i < placedComponents.size(); i++) {
			double[] size = sizes.get(i);
			if (x > 0 && x + size[0] > targetWidth) {
				y += shelfHeight + COMPONENT_GAP;
				x = 0;
				shelfHeight = 0;
			}
			for (Map.Entry<String, Position> entry : placedComponents.get(i).entrySet()) {
				positions.put(entry.getKey(), new Position(x + entry.getValue().x(), y + entry.getValue().y()));
				componentOf.put(entry.getKey(), i);
			}
			packedWidth = Math.max(packedWidth, x + size[0]);
			x += size[0] + COMPONENT_GAP;
			shelfHeight = Math.max(shelfHeight, size[1]);
		}
		double packedBottom = placedComponents.isEmpty() ? 0 : y + shelfHeight;

		// the nodes without links, as one block grouped by type
		List<LayoutNode> isolated = new ArrayList<>();
		for (LayoutNode node : byId.values()) {
			if (!linked.contains(node.id())) {
				isolated.add(node);
			}
		}
		isolated.sort(Comparator.comparing((LayoutNode node) -> Objects.toString(node.type(), ""))
				.thenComparing(node -> node.name() == null ? node.id() : node.name(), String.CASE_INSENSITIVE_ORDER)
				.thenComparing(LayoutNode::id));
		if (!isolated.isEmpty()) {
			double blockWidth = placedComponents.isEmpty()
					? Math.sqrt(isolated.size() * CELL_WIDTH * CELL_HEIGHT * ASPECT)
					: Math.max(packedWidth, CELL_WIDTH);
			int columns = Math.max(1, (int) Math.floor((blockWidth + NODE_GAP) / CELL_WIDTH));
			double top = placedComponents.isEmpty() ? 0 : packedBottom + COMPONENT_GAP;
			int row = 0;
			int column = 0;
			String type = null;
			boolean first = true;
			for (LayoutNode node : isolated) {
				String nodeType = Objects.toString(node.type(), "");
				// each type starts its own row
				if (!first && (column >= columns || !nodeType.equals(type))) {
					row++;
					column = 0;
				}
				first = false;
				type = nodeType;
				positions.put(node.id(), new Position(column * CELL_WIDTH, top + row * CELL_HEIGHT));
				componentOf.put(node.id(), -1);
				column++;
			}
		}

		// normalized so the top left is 0, 0
		double minX = Double.POSITIVE_INFINITY;
		double minY = Double.POSITIVE_INFINITY;
		for (Position position : positions.values()) {
			minX = Math.min(minX, position.x());
			minY = Math.min(minY, position.y());
		}
		double width = 0;
		double height = 0;
		if (!positions.isEmpty()) {
			for (Map.Entry<String, Position> entry : positions.entrySet()) {
				Position moved = new Position(entry.getValue().x() - minX, entry.getValue().y() - minY);
				entry.setValue(moved);
				width = Math.max(width, moved.x() + NODE_WIDTH);
				height = Math.max(height, moved.y() + NODE_HEIGHT);
			}
		}
		graph.clear();
		return new Result(positions, componentOf, inCycle, cycles, components.size(), isolated.size(), width,
				height);
	}

	/**
	 * Lays out one component with a seeded SpringBox and separates its boxes
	 *
	 * @return each node's top left corner, with the component's top left at 0, 0
	 */
	private static Map<String, Position> layoutComponent(List<String> ids, Collection<LayoutEdge> allEdges,
			int gridThreshold) {
		Map<String, Position> placed = new LinkedHashMap<>();
		if (ids.size() == 1) {
			placed.put(ids.get(0), new Position(0, 0));
			return placed;
		}
		Set<String> members = new HashSet<>(ids);
		Graph graph = new MultiGraph("component");
		Layout layout = new SpringBox(false, new Random(seedOf(ids)));
		graph.addSink(layout);
		layout.addAttributeSink(graph);
		for (String id : ids) {
			graph.addNode(id);
		}
		for (LayoutEdge edge : allEdges) {
			if (!edge.source().equals(edge.target()) && members.contains(edge.source())) {
				graph.addEdge(edge.id(), edge.source(), edge.target());
			}
		}
		GenerateMetamodelLayout.computeLayout(layout);

		Map<String, double[]> scaled = new LinkedHashMap<>();
		for (String id : ids) {
			double[] position = Toolkit.nodePosition(graph.getNode(id));
			// SpringBox's y grows upwards, the page's downwards
			scaled.put(id, new double[] { position[0] * SCALE_X, -position[1] * SCALE_Y });
		}
		graph.removeSink(layout);
		layout.removeAttributeSink(graph);
		graph.clear();

		if (ids.size() > gridThreshold) {
			placed.putAll(snapToGrid(scaled));
		} else {
			Map<String, Rectangle2D> rectangles = new LinkedHashMap<>();
			for (Map.Entry<String, double[]> entry : scaled.entrySet()) {
				rectangles.put(entry.getKey(), new Rectangle2D.Double(entry.getValue()[0], entry.getValue()[1],
						CELL_WIDTH, CELL_HEIGHT));
			}
			Map<String, Rectangle2D> fixed = new Rectangles().fix(rectangles);
			for (String id : ids) {
				Rectangle2D rectangle = fixed.get(id);
				placed.put(id, new Position(rectangle.getX(), rectangle.getY()));
			}
		}
		return normalize(placed);
	}

	/**
	 * Snaps each node to the nearest cell of a grid of node sized cells, moving one
	 * whose cell is taken to the nearest free one. Nodes are placed from the top
	 * left, so the result does not depend on the map's order.
	 */
	static Map<String, Position> snapToGrid(Map<String, double[]> scaled) {
		List<Map.Entry<String, double[]>> order = new ArrayList<>(scaled.entrySet());
		order.sort(Comparator.comparingDouble((Map.Entry<String, double[]> entry) -> entry.getValue()[1])
				.thenComparingDouble(entry -> entry.getValue()[0]).thenComparing(Map.Entry::getKey));
		Set<Long> taken = new HashSet<>();
		Map<String, Position> placed = new LinkedHashMap<>();
		for (Map.Entry<String, double[]> entry : order) {
			double column = entry.getValue()[0] / CELL_WIDTH;
			double row = entry.getValue()[1] / CELL_HEIGHT;
			long baseColumn = Math.round(column);
			long baseRow = Math.round(row);
			long[] cell = null;
			if (taken.add(cellKey(baseRow, baseColumn))) {
				cell = new long[] { baseRow, baseColumn };
			}
			for (int ring = 1; cell == null; ring++) {
				double best = Double.POSITIVE_INFINITY;
				for (int dr = -ring; dr <= ring; dr++) {
					for (int dc = -ring; dc <= ring; dc++) {
						if (Math.max(Math.abs(dr), Math.abs(dc)) != ring) {
							continue;
						}
						long r = baseRow + dr;
						long c = baseColumn + dc;
						if (taken.contains(cellKey(r, c))) {
							continue;
						}
						double distance = (r - row) * (r - row) + (c - column) * (c - column);
						if (distance < best) {
							best = distance;
							cell = new long[] { r, c };
						}
					}
				}
				if (cell != null) {
					taken.add(cellKey(cell[0], cell[1]));
				}
			}
			placed.put(entry.getKey(), new Position(cell[1] * CELL_WIDTH, cell[0] * CELL_HEIGHT));
		}
		return placed;
	}

	private static long cellKey(long row, long column) {
		return (row << 32) ^ (column & 0xffffffffL);
	}

	private static Map<String, Position> normalize(Map<String, Position> placed) {
		double minX = Double.POSITIVE_INFINITY;
		double minY = Double.POSITIVE_INFINITY;
		for (Position position : placed.values()) {
			minX = Math.min(minX, position.x());
			minY = Math.min(minY, position.y());
		}
		Map<String, Position> normalized = new LinkedHashMap<>();
		for (Map.Entry<String, Position> entry : placed.entrySet()) {
			normalized.put(entry.getKey(),
					new Position(entry.getValue().x() - minX, entry.getValue().y() - minY));
		}
		return normalized;
	}

	/** The width and height of a component's boxes, from 0, 0 */
	private static double[] extent(Map<String, Position> placed) {
		double width = 0;
		double height = 0;
		for (Position position : placed.values()) {
			width = Math.max(width, position.x() + NODE_WIDTH);
			height = Math.max(height, position.y() + NODE_HEIGHT);
		}
		return new double[] { width, height };
	}

	/** A seed from a component's sorted ids (64 bit FNV-1a), the same on every run */
	static long seedOf(List<String> sortedIds) {
		long hash = 0xcbf29ce484222325L;
		for (String id : sortedIds) {
			for (byte b : id.getBytes(StandardCharsets.UTF_8)) {
				hash ^= b & 0xff;
				hash *= 0x100000001b3L;
			}
			hash ^= '\n';
			hash *= 0x100000001b3L;
		}
		return hash;
	}

	/**
	 * The nodes reached from a root by following the links: what it depends on,
	 * what depends on it, or both, each followed on its own so BOTH does not wander
	 * from a dependency up to its other dependents.
	 *
	 * @param edges     the links
	 * @param root      the node to start from, always in the result
	 * @param direction which way to follow the links
	 * @param depth     how many links to follow at most; null for no limit
	 * @return the root and the nodes reached, in the order they were reached
	 */
	public static Set<String> trace(Collection<LayoutEdge> edges, String root, Direction direction, Integer depth) {
		if (depth != null && depth < 0) {
			throw new IllegalArgumentException("The depth cannot be negative");
		}
		Map<String, List<String>> dependencies = new TreeMap<>();
		Map<String, List<String>> dependents = new TreeMap<>();
		for (LayoutEdge edge : edges) {
			dependencies.computeIfAbsent(edge.source(), key -> new ArrayList<>()).add(edge.target());
			dependents.computeIfAbsent(edge.target(), key -> new ArrayList<>()).add(edge.source());
		}
		Set<String> reached = new LinkedHashSet<>();
		reached.add(root);
		if (direction != Direction.DEPENDENTS) {
			reached.addAll(breadthFirst(dependencies, root, depth));
		}
		if (direction != Direction.DEPENDENCIES) {
			reached.addAll(breadthFirst(dependents, root, depth));
		}
		return reached;
	}

	private static Set<String> breadthFirst(Map<String, List<String>> next, String root, Integer depth) {
		Map<String, Integer> distance = new LinkedHashMap<>();
		distance.put(root, 0);
		ArrayDeque<String> queue = new ArrayDeque<>();
		queue.add(root);
		while (!queue.isEmpty()) {
			String id = queue.poll();
			int at = distance.get(id);
			if (depth != null && at >= depth) {
				continue;
			}
			List<String> neighbours = new ArrayList<>(next.getOrDefault(id, List.of()));
			Collections.sort(neighbours);
			for (String neighbour : neighbours) {
				if (!distance.containsKey(neighbour)) {
					distance.put(neighbour, at + 1);
					queue.add(neighbour);
				}
			}
		}
		return distance.keySet();
	}

	/**
	 * The links among a set of nodes
	 *
	 * @param edges the links
	 * @param nodes the node ids to keep
	 * @return the links whose ends are both kept
	 */
	public static List<LayoutEdge> edgesAmong(Collection<LayoutEdge> edges, Set<String> nodes) {
		List<LayoutEdge> among = new ArrayList<>();
		for (LayoutEdge edge : edges) {
			if (nodes.contains(edge.source()) && nodes.contains(edge.target())) {
				among.add(edge);
			}
		}
		return among;
	}

}
