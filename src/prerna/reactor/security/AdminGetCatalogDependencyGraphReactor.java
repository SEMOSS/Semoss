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
package prerna.reactor.security;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.CatalogDependencyGraphLayout;
import prerna.auth.utils.CatalogDependencyGraphLayout.Direction;
import prerna.auth.utils.CatalogDependencyGraphLayout.LayoutEdge;
import prerna.auth.utils.CatalogDependencyGraphLayout.LayoutNode;
import prerna.auth.utils.CatalogDependencyGraphLayout.Position;
import prerna.auth.utils.CatalogDependencyRole;
import prerna.auth.utils.CatalogTypes;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Admin only: the catalog dependency graph of the whole platform, every engine
 * and project with every link between them, laid out for the dependency map.
 * With a catalog id, only what that item reaches in the given direction, up to
 * the given depth.
 *
 * <p>
 * Reads the security database in a fixed number of queries whatever the size of
 * the graph: every link, every engine, every project, and the descriptions of the
 * items shown in chunks.
 */
public class AdminGetCatalogDependencyGraphReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminGetCatalogDependencyGraphReactor.class);

	private static final String DIRECTION = "direction";
	private static final String DEPTH = "depth";

	private static final String ENGINE = "ENGINE";
	private static final String PROJECT = "PROJECT";
	/** How many ids one metadata query filters on */
	private static final int METADATA_CHUNK = 500;
	private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public AdminGetCatalogDependencyGraphReactor() {
		this.keysToGet = new String[] { CatalogDependencyKeys.CATALOG_ID, DIRECTION, DEPTH };
		this.keyRequired = new int[] { 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User is not an admin and does not have access. Please login as an admin");
		}
		String catalogId = this.keyValue.get(CatalogDependencyKeys.CATALOG_ID);
		if (catalogId != null && catalogId.isBlank()) {
			catalogId = null;
		}
		Direction direction = Direction.BOTH;
		Integer depth = null;
		if (catalogId != null) {
			direction = Direction.parse(this.keyValue.get(DIRECTION));
			depth = parseDepth(this.keyValue.get(DEPTH));
		}
		return new NounMetadata(buildGraph(catalogId == null ? null : catalogId.trim(), direction, depth),
				PixelDataType.MAP);
	}

	private static Integer parseDepth(String value) {
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			int depth = (int) Double.parseDouble(value.trim());
			if (depth < 0) {
				throw new IllegalArgumentException("The depth cannot be negative");
			}
			return depth;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("The depth must be a whole number, not '" + value + "'");
		}
	}

	/** A row of CATALOGDEPENDENCIES */
	private record Link(String catalogId, String catalogType, String dependencyId, String dependencyType,
			String userId, String dateAdded, boolean automatic, String role, String name, String description) {

		String edgeId() {
			return catalogId + "|" + dependencyId + "|" + role;
		}
	}

	/** A row of ENGINE or PROJECT */
	private record Item(String id, String kind, String type, String subtype, String name, boolean discoverable,
			boolean global, String createdBy, String dateCreated) {
	}

	/**
	 * The graph, laid out
	 *
	 * @param catalogId the item to start from, or null for the whole platform
	 * @param direction which way to follow its links
	 * @param depth     how many links to follow at most; null for no limit
	 * @return nodes, edges, stats, bounds and nodeSize
	 */
	public static Map<String, Object> buildGraph(String catalogId, Direction direction, Integer depth) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Link> links = readLinks(securityDb);
		Map<String, Item> items = new HashMap<>();
		for (Item item : readItems(securityDb, ENGINE)) {
			items.put(item.id(), item);
		}
		for (Item item : readItems(securityDb, PROJECT)) {
			items.putIfAbsent(item.id(), item);
		}

		// one link per owner, target and role
		Map<String, Link> linksById = new TreeMap<>();
		for (Link link : links) {
			if (link.catalogId() == null || link.dependencyId() == null) {
				continue;
			}
			linksById.putIfAbsent(link.edgeId(), link);
		}
		List<LayoutEdge> allEdges = new ArrayList<>(linksById.size());
		for (Map.Entry<String, Link> entry : linksById.entrySet()) {
			allEdges.add(new LayoutEdge(entry.getKey(), entry.getValue().catalogId(),
					entry.getValue().dependencyId()));
		}

		// counts across the whole platform, whatever is shown
		Map<String, Set<String>> dependenciesOf = new HashMap<>();
		Map<String, Set<String>> dependentsOf = new HashMap<>();
		// the kind and type of an id that is in neither ENGINE nor PROJECT, from a link
		Map<String, String> linkedType = new HashMap<>();
		for (Link link : linksById.values()) {
			dependenciesOf.computeIfAbsent(link.catalogId(), key -> new HashSet<>()).add(link.dependencyId());
			dependentsOf.computeIfAbsent(link.dependencyId(), key -> new HashSet<>()).add(link.catalogId());
			linkedType.putIfAbsent(link.dependencyId(), link.dependencyType());
			linkedType.putIfAbsent(link.catalogId(), link.catalogType());
		}

		Set<String> nodeIds;
		List<LayoutEdge> edges;
		if (catalogId == null) {
			nodeIds = new TreeSet<>(items.keySet());
			nodeIds.addAll(linkedType.keySet());
			edges = allEdges;
		} else {
			if (!items.containsKey(catalogId) && !linkedType.containsKey(catalogId)) {
				throw new IllegalArgumentException("No engine or project has the id " + catalogId);
			}
			nodeIds = new TreeSet<>(CatalogDependencyGraphLayout.trace(allEdges, catalogId, direction, depth));
			edges = CatalogDependencyGraphLayout.edgesAmong(allEdges, nodeIds);
		}

		Map<String, Item> nodeItems = new LinkedHashMap<>();
		for (String id : nodeIds) {
			Item item = items.get(id);
			if (item == null) {
				String type = linkedType.get(id);
				item = new Item(id, kindOfType(type), normalizedType(type), null, id, false, false, null, null);
			}
			nodeItems.put(id, item);
		}

		List<LayoutNode> layoutNodes = new ArrayList<>(nodeItems.size());
		for (Item item : nodeItems.values()) {
			layoutNodes.add(new LayoutNode(item.id(), item.kind() + ":" + item.type(), item.name()));
		}
		CatalogDependencyGraphLayout.Result layout = CatalogDependencyGraphLayout.layout(layoutNodes, edges);

		Map<String, String> descriptions = readDescriptions(nodeItems.values(), items.keySet());

		List<Map<String, Object>> nodes = new ArrayList<>(nodeItems.size());
		int missing = 0;
		for (Item item : nodeItems.values()) {
			boolean isMissing = !items.containsKey(item.id());
			if (isMissing) {
				missing++;
			}
			Position position = layout.positions().get(item.id());
			Map<String, Object> node = new LinkedHashMap<>();
			node.put("id", item.id());
			node.put("kind", item.kind());
			node.put("type", item.type());
			node.put("subtype", item.subtype());
			node.put("name", item.name());
			node.put("description", descriptions.get(item.id()));
			node.put("discoverable", item.discoverable());
			node.put("global", item.global());
			node.put("createdBy", item.createdBy());
			node.put("dateCreated", item.dateCreated());
			node.put("missing", isMissing);
			node.put("x", position.x());
			node.put("y", position.y());
			node.put("component", layout.components().get(item.id()));
			node.put("dependencyCount", dependenciesOf.getOrDefault(item.id(), Set.of()).size());
			node.put("dependentCount", dependentsOf.getOrDefault(item.id(), Set.of()).size());
			nodes.add(node);
		}

		List<Map<String, Object>> edgeRows = new ArrayList<>(edges.size());
		int automatic = 0;
		for (LayoutEdge edge : edges) {
			Link link = linksById.get(edge.id());
			if (link.automatic()) {
				automatic++;
			}
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("id", edge.id());
			row.put("source", link.catalogId());
			row.put("target", link.dependencyId());
			row.put("role", link.role());
			row.put("isAutomatic", link.automatic());
			row.put("name", link.name());
			row.put("description", link.description());
			row.put("dateAdded", link.dateAdded());
			row.put("addedBy", link.userId());
			row.put("inCycle", layout.inCycleEdges().contains(edge.id()));
			edgeRows.add(row);
		}

		Map<String, Object> stats = new LinkedHashMap<>();
		stats.put("nodes", nodes.size());
		stats.put("edges", edgeRows.size());
		stats.put("automatic", automatic);
		stats.put("manual", edgeRows.size() - automatic);
		stats.put("missing", missing);
		stats.put("isolated", layout.isolatedCount());
		stats.put("cycles", layout.cycles());
		stats.put("components", layout.componentCount());

		Map<String, Object> bounds = new LinkedHashMap<>();
		bounds.put("width", layout.width());
		bounds.put("height", layout.height());
		Map<String, Object> nodeSize = new LinkedHashMap<>();
		nodeSize.put("width", (int) CatalogDependencyGraphLayout.NODE_WIDTH);
		nodeSize.put("height", (int) CatalogDependencyGraphLayout.NODE_HEIGHT);

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("nodes", nodes);
		result.put("edges", edgeRows);
		result.put("stats", stats);
		result.put("bounds", bounds);
		result.put("nodeSize", nodeSize);
		return result;
	}

	/**
	 * ENGINE or PROJECT for a stored type, guessed for an id no longer in either
	 * table: a project type is a project, anything else an engine
	 */
	static String kindOfType(String type) {
		if (type == null || type.isBlank()) {
			return ENGINE;
		}
		if (PROJECT.equalsIgnoreCase(type.trim())) {
			return PROJECT;
		}
		try {
			return CatalogTypes.kindOf(type).name();
		} catch (IllegalArgumentException e) {
			return ENGINE;
		}
	}

	private static String normalizedType(String type) {
		if (type == null || type.isBlank()) {
			return null;
		}
		try {
			return CatalogTypes.normalize(type);
		} catch (IllegalArgumentException e) {
			return type.trim();
		}
	}

	private static List<Link> readLinks(IRDBMSEngine securityDb) {
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String column : new String[] { "CATALOGID", "CATALOGTYPE", "DEPENDENCYID", "DEPENDENCYTYPE", "USERID",
				"DATEADDED", "IS_AUTOMATIC", "ROLE", "NAME", "DESCRIPTION" }) {
			qs.addSelector(new QueryColumnSelector("CATALOGDEPENDENCIES__" + column));
		}
		try {
			return QueryExecutionUtility.queryList(securityDb, qs, rs -> {
				boolean automatic = rs.getBoolean(7);
				// as SecurityCatalogDependencyUtils reads a link: a row from before
				// IS_AUTOMATIC is a picked one, and an automatic one has no name
				String role = automatic ? CatalogDependencyRole.fromStored(rs.getString(8)).name()
						: CatalogDependencyRole.MANUAL.name();
				return new Link(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
						format(rs.getTimestamp(6)), automatic, role, automatic ? null : rs.getString(9),
						rs.getString(10));
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the catalog dependencies", e);
		}
	}

	private static List<Item> readItems(IRDBMSEngine securityDb, String table) {
		boolean engine = ENGINE.equals(table);
		String prefix = table + "__";
		SelectQueryStruct qs = new SelectQueryStruct();
		List<String> columns = engine
				? List.of("ENGINEID", "ENGINENAME", "ENGINEDISPLAYNAME", "ENGINETYPE", "DISCOVERABLE", "GLOBAL",
						"CREATEDBY", "DATECREATED", "ENGINESUBTYPE")
				// a project has no subtype
				: List.of("PROJECTID", "PROJECTNAME", "PROJECTDISPLAYNAME", "TYPE", "DISCOVERABLE", "GLOBAL",
						"CREATEDBY", "DATECREATED");
		for (String column : columns) {
			qs.addSelector(new QueryColumnSelector(prefix + column));
		}
		try {
			return QueryExecutionUtility.queryList(securityDb, qs, rs -> {
				String name = rs.getString(3);
				if (name == null || name.isBlank()) {
					name = rs.getString(2);
				}
				String type = engine ? rs.getString(4) : CatalogTypes.projectTypeOf(rs.getString(4));
				String subtype = engine ? rs.getString(9) : null;
				return new Item(rs.getString(1), table, type, subtype, name, rs.getBoolean(5), rs.getBoolean(6),
						rs.getString(7), format(rs.getTimestamp(8)));
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the " + table.toLowerCase() + " catalog", e);
		}
	}

	/**
	 * The description of each item shown, from ENGINEMETA or PROJECTMETA, read a
	 * chunk of ids at a time. An item with several takes the first by METAORDER.
	 */
	private static Map<String, String> readDescriptions(Collection<Item> shown, Set<String> known) {
		List<String> engineIds = new ArrayList<>();
		List<String> projectIds = new ArrayList<>();
		for (Item item : shown) {
			if (!known.contains(item.id())) {
				continue;
			}
			(ENGINE.equals(item.kind()) ? engineIds : projectIds).add(item.id());
		}
		Map<String, String> descriptions = new HashMap<>();
		Map<String, Double> orders = new HashMap<>();
		for (int start = 0; start < engineIds.size(); start += METADATA_CHUNK) {
			List<String> chunk = engineIds.subList(start, Math.min(engineIds.size(), start + METADATA_CHUNK));
			try (IRawSelectWrapper wrapper = SecurityEngineUtils.getEngineMetadataWrapper(chunk,
					List.of(Constants.DESCRIPTION), true)) {
				collectDescriptions(wrapper, descriptions, orders);
			} catch (Exception e) {
				classLogger.warn("Could not read the descriptions of {} engines", chunk.size(), e);
			}
		}
		for (int start = 0; start < projectIds.size(); start += METADATA_CHUNK) {
			List<String> chunk = projectIds.subList(start, Math.min(projectIds.size(), start + METADATA_CHUNK));
			try (IRawSelectWrapper wrapper = SecurityProjectUtils.getProjectMetadataWrapper(chunk,
					List.of(Constants.DESCRIPTION), true)) {
				collectDescriptions(wrapper, descriptions, orders);
			} catch (Exception e) {
				classLogger.warn("Could not read the descriptions of {} projects", chunk.size(), e);
			}
		}
		return descriptions;
	}

	private static void collectDescriptions(IRawSelectWrapper wrapper, Map<String, String> descriptions,
			Map<String, Double> orders) {
		while (wrapper.hasNext()) {
			Object[] values = wrapper.next().getValues();
			if (values[0] == null || values[2] == null) {
				continue;
			}
			String id = values[0].toString();
			double order = values[3] instanceof Number number ? number.doubleValue() : Double.MAX_VALUE;
			Double previous = orders.get(id);
			if (previous == null || order < previous) {
				orders.put(id, order);
				descriptions.put(id, values[2].toString());
			}
		}
	}

	private static String format(Timestamp timestamp) {
		return timestamp == null ? null : timestamp.toLocalDateTime().format(DATE_FORMAT);
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition()
				.description("Admin Only: The catalog dependency graph, laid out for the dependency map. With no "
						+ "catalog id, every engine and project on the platform, linked or not, and every link; with "
						+ "one, the items it reaches in the given direction up to the given depth, and the links "
						+ "among them. Each node has its top left x and y in pixels and its component; each link "
						+ "whether it is part of a cycle.")
				.key(CatalogDependencyKeys.CATALOG_ID, "Optional: the engine id or project id to start from")
				.key(DIRECTION, "Optional, with a catalog id: DEPENDENCIES, DEPENDENTS or BOTH (the default)")
				.key(DEPTH, "Optional, with a catalog id: how many links to follow at most; no limit by default")
				.output(Map.class, "nodes, edges, stats, bounds and nodeSize");
	}
}
