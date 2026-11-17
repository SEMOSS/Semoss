package prerna.auth.utils;

import java.io.File;

import com.google.gson.reflect.TypeToken;
import prerna.util.gson.GsonUtility;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionErrorRules;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.date.SemossDate;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.notifications.NotificationDbUtils;
import prerna.project.api.IProject;
import prerna.project.impl.ProjectHelper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.parser.ParserException;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.EmailUtility;
import prerna.util.InsightsRDBMSUtils;
import prerna.util.InternalUtility;
import prerna.util.NotificationConstants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import java.util.LinkedHashMap;

/**
 * The dependencies of catalog items, kept in CATALOGDEPENDENCIES: an engine or
 * a project (the catalog item, or owner) and the engines and projects it depends
 * on. Both are kept with their own engine type or project type (see
 * {@link CatalogTypes}), never a generic ENGINE or PROJECT.
 *
 * <p>
 * Everything here reads and writes the security database alone. Nothing loads an
 * engine or a project, or pulls one from cloud storage, so it works the same on
 * every server of a cluster, whichever items that server has loaded.
 * </p>
 */
public class SecurityCatalogDependencyUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityCatalogDependencyUtils.class);

	private static final String TABLE = "CATALOGDEPENDENCIES";

	/**
	 * One dependency of a catalog item: the engine or project it depends on, and
	 * that item's own type.
	 *
	 * @param id   the engine id or project id
	 * @param type its engine type or project type, read with
	 *             {@link CatalogTypes#normalize}
	 */
	public record Dependency(String id, String type) {
		public Dependency {
			if (id == null || id.isBlank()) {
				throw new IllegalArgumentException("A dependency id is required");
			}
			type = CatalogTypes.normalize(type);
		}
	}

	SecurityCatalogDependencyUtils() {

	}

	/**
	 * The filter that a dependency is a project, for choosing between the ENGINE
	 * and PROJECT columns of a dependency
	 */
	private static SimpleQueryFilter isProjectDependency() {
		return SimpleQueryFilter.makeColToValFilter("CATALOGDEPENDENCIES__DEPENDENCYTYPE", "==",
				new ArrayList<>(CatalogTypes.projectTypes()));
	}

	/*
	 * Types and permissions
	 */

	/**
	 * The type of a catalog item: the one given, checked against the security
	 * database, or the one the security database has when none is given.
	 *
	 * @param catalogId   an engine id or project id
	 * @param catalogType its type, or null to read it
	 * @return its engine type or project type
	 * @throws IllegalArgumentException when the security database has no such item,
	 *                                  or the type given is not its type
	 */
	public static String resolveCatalogType(String catalogId, String catalogType) {
		String stored = CatalogTypes.resolve(catalogId);
		if (stored == null) {
			throw new IllegalArgumentException("No engine or project has the id " + catalogId);
		}
		if (catalogType == null || catalogType.isBlank()) {
			return stored;
		}
		// a project's type is read as PROJECT.TYPE is, so a legacy or missing one is
		// INSIGHTS, the same as the stored type
		String given = CatalogTypes.isProjectType(stored) && !CatalogTypes.engineTypes()
				.contains(catalogType.trim().toUpperCase(java.util.Locale.ROOT)) ? CatalogTypes.projectTypeOf(catalogType)
						: CatalogTypes.normalize(catalogType);
		if (!stored.equals(given)) {
			throw new IllegalArgumentException(catalogId + " is a " + stored + ", not a " + given);
		}
		return stored;
	}

	/**
	 * Whether a user can view a catalog item
	 *
	 * @param user        the user
	 * @param catalogId   an engine id or project id
	 * @param catalogType its engine type or project type
	 * @return whether they can view it
	 */
	public static boolean userCanView(User user, String catalogId, String catalogType) {
		if (CatalogTypes.isProjectType(catalogType)) {
			return SecurityProjectUtils.userCanViewProject(user, catalogId);
		}
		return SecurityEngineUtils.userCanViewEngine(user, catalogId);
	}

	/**
	 * Whether a user can edit a catalog item, and so its dependencies
	 *
	 * @param user        the user
	 * @param catalogId   an engine id or project id
	 * @param catalogType its engine type or project type
	 * @return whether they can edit it
	 */
	public static boolean userCanEdit(User user, String catalogId, String catalogType) {
		if (CatalogTypes.isProjectType(catalogType)) {
			return SecurityProjectUtils.userCanEditProject(user, catalogId);
		}
		return SecurityEngineUtils.userCanEditEngine(user, catalogId);
	}

	/**
	 * Reads the types of engine ids and project ids from the security database.
	 * Never loads or pulls an engine or project.
	 *
	 * @param ids    engine ids and project ids
	 * @param failed collects the ids that are in neither table, when not null
	 * @return the dependencies of the ids found, in order, once each
	 */
	public static List<Dependency> resolveDependencies(Collection<String> ids, Collection<String> failed) {
		List<Dependency> dependencies = new ArrayList<>();
		Set<String> seen = new HashSet<>();
		for (String id : ids) {
			if (id == null || !seen.add(id)) {
				continue;
			}
			String type = CatalogTypes.resolve(id);
			if (type == null) {
				classLogger.warn("{} is not an engine or a project, so it is not a dependency", id);
				if (failed != null) {
					failed.add(id);
				}
				continue;
			}
			dependencies.add(new Dependency(id, type));
		}
		return dependencies;
	}

	/*
	 * Writes
	 */

	/**
	 * Replaces all the dependencies of a catalog item, in one transaction.
	 *
	 * @param user         who sets them
	 * @param catalogId    the engine id or project id
	 * @param catalogType  its engine type or project type
	 * @param dependencies its dependencies; an empty list removes them all
	 * @throws IllegalStateException when they cannot be saved; nothing is changed
	 */
	public static void setDependencies(User user, String catalogId, String catalogType,
			Collection<Dependency> dependencies) {
		String ownerType = CatalogTypes.normalize(catalogType);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				QueryExecutionUtility.executeUpdate(connection, "DELETE FROM " + TABLE + " WHERE CATALOGID=?",
						ps -> ps.setString(1, catalogId));
				insert(connection, user, catalogId, ownerType, withoutOwnerAndRepeats(catalogId, dependencies));
				return null;
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to set the dependencies of {}", catalogId, e);
			throw new IllegalStateException("Unable to save the dependencies of " + catalogId, e);
		}
	}

	/**
	 * Replaces all the dependencies of a catalog item with the engines and projects
	 * of these ids, reading their types from the security database. Ids that are
	 * neither are left out.
	 *
	 * @param user        who sets them
	 * @param catalogId   the engine id or project id
	 * @param catalogType its engine type or project type
	 * @param ids         the engine ids and project ids it depends on
	 * @return the ids left out, which are neither an engine nor a project
	 */
	public static List<String> setDependencyIds(User user, String catalogId, String catalogType,
			Collection<String> ids) {
		List<String> failed = new ArrayList<>();
		setDependencies(user, catalogId, catalogType, resolveDependencies(ids, failed));
		return failed;
	}

	/**
	 * Adds dependencies to a catalog item, keeping the ones it has, in one
	 * transaction.
	 *
	 * @param user         who adds them
	 * @param catalogId    the engine id or project id
	 * @param catalogType  its engine type or project type
	 * @param dependencies the dependencies to add; ones it has already are skipped
	 * @throws IllegalStateException when they cannot be saved; nothing is changed
	 */
	public static void addDependencies(User user, String catalogId, String catalogType,
			Collection<Dependency> dependencies) {
		String ownerType = CatalogTypes.normalize(catalogType);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				Set<String> existing = new HashSet<>(QueryExecutionUtility.queryList(connection,
						"SELECT DEPENDENCYID FROM " + TABLE + " WHERE CATALOGID=?", ps -> ps.setString(1, catalogId),
						rs -> rs.getString(1)));
				List<Dependency> toAdd = new ArrayList<>();
				for (Dependency dependency : withoutOwnerAndRepeats(catalogId, dependencies)) {
					if (!existing.contains(dependency.id())) {
						toAdd.add(dependency);
					}
				}
				insert(connection, user, catalogId, ownerType, toAdd);
				return null;
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to add dependencies to {}", catalogId, e);
			throw new IllegalStateException("Unable to save the dependencies of " + catalogId, e);
		}
	}

	/**
	 * Removes one dependency from a catalog item.
	 *
	 * @param user         who removes it; they must be able to edit the item
	 * @param catalogId    the engine id or project id
	 * @param dependencyId the engine id or project id it no longer depends on
	 * @throws IllegalAccessException when the user cannot edit the item
	 */
	public static void removeDependency(User user, String catalogId, String dependencyId)
			throws IllegalAccessException {
		String catalogType = resolveCatalogType(catalogId, null);
		if (!userCanEdit(user, catalogId, catalogType)) {
			throw new IllegalAccessException("User does not have permissions to remove dependencies");
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			QueryExecutionUtility.executeUpdate(securityDb,
					"DELETE FROM " + TABLE + " WHERE CATALOGID=? AND DEPENDENCYID=?", ps -> {
						ps.setString(1, catalogId);
						ps.setString(2, dependencyId);
					});
		} catch (Exception e) {
			classLogger.error("Failed to remove the dependency of {} on {}", catalogId, dependencyId, e);
			throw new IllegalStateException("Unable to remove the dependency on " + dependencyId, e);
		}
	}

	/**
	 * Removes the dependencies a deleted engine or project owned. The rows of other
	 * items that depend on it are kept, so validating them shows it is gone.
	 *
	 * @param catalogId the engine id or project id
	 */
	public static void deleteDependenciesOwnedBy(String catalogId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			QueryExecutionUtility.executeUpdate(securityDb, "DELETE FROM " + TABLE + " WHERE CATALOGID=?",
					ps -> ps.setString(1, catalogId));
		} catch (Exception e) {
			classLogger.error("Failed to delete the dependencies of {}", catalogId, e);
		}
	}

	/*
	 * Export and import
	 */

	/**
	 * Reads the dependency file of an exported project, when it has one.
	 *
	 * @param file the {@code <name>_dependencies.json} file
	 * @return its rows, or an empty list when there is no file or it cannot be read
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> readDependencyFile(File file) {
		if (file == null || !file.isFile()) {
			return new ArrayList<>();
		}
		try {
			List<Map<String, Object>> rows = (List<Map<String, Object>>) GsonUtility.readJsonFileToObject(file,
					new TypeToken<List<Map<String, Object>>>() {
					}.getType());
			return rows == null ? new ArrayList<>() : rows;
		} catch (Exception e) {
			classLogger.warn("Unable to read the dependencies in {}", file.getName(), e);
			return new ArrayList<>();
		}
	}

	/**
	 * Reads the rows of an exported dependency file as dependencies. Exports from
	 * this release give catalog_id and catalog_type; older ones give engine_id and
	 * engine_type, where a project's type is PROJECT. Each type is read from the
	 * security database when it has the item. Otherwise the exported type is kept
	 * when it is an engine type or project type, and the row is left out when it
	 * is not, such as an old PROJECT.
	 *
	 * @param rows the rows of an exported dependency file
	 * @return the dependencies, in order
	 */
	public static List<Dependency> fromExported(List<Map<String, Object>> rows) {
		List<Dependency> dependencies = new ArrayList<>();
		if (rows == null) {
			return dependencies;
		}
		for (Map<String, Object> row : rows) {
			Object idValue = row.containsKey("catalog_id") ? row.get("catalog_id") : row.get("engine_id");
			Object typeValue = row.containsKey("catalog_type") ? row.get("catalog_type") : row.get("engine_type");
			if (!(idValue instanceof String id) || id.isBlank()) {
				continue;
			}
			String type = CatalogTypes.resolve(id);
			if (type == null && typeValue instanceof String exported) {
				try {
					type = CatalogTypes.normalize(exported);
				} catch (IllegalArgumentException e) {
					type = null;
				}
			}
			if (type == null) {
				classLogger.warn("Left out the exported dependency on {}: its type is not known", id);
				continue;
			}
			dependencies.add(new Dependency(id, type));
		}
		return dependencies;
	}

	private static List<Dependency> withoutOwnerAndRepeats(String catalogId, Collection<Dependency> dependencies) {
		List<Dependency> kept = new ArrayList<>();
		if (dependencies == null) {
			return kept;
		}
		Set<String> seen = new HashSet<>();
		for (Dependency dependency : dependencies) {
			if (dependency.id().equals(catalogId)) {
				classLogger.warn("{} cannot depend on itself", catalogId);
				continue;
			}
			if (seen.add(dependency.id())) {
				kept.add(dependency);
			}
		}
		return kept;
	}

	private static void insert(Connection connection, User user, String catalogId, String catalogType,
			List<Dependency> dependencies) throws Exception {
		if (dependencies.isEmpty()) {
			return;
		}
		AccessToken token = user == null ? null : user.getPrimaryLoginToken();
		String userId = token == null ? null : token.getId();
		String userType = token == null ? null : token.getProvider().getLabel();
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String query = "INSERT INTO " + TABLE
				+ " (CATALOGID, CATALOGTYPE, DEPENDENCYID, DEPENDENCYTYPE, USERID, TYPE, DATEADDED) VALUES (?, ?, ?, ?, ?, ?, ?)";
		QueryExecutionUtility.executeBatch(connection, query, dependencies, (ps, dependency) -> {
			ps.setString(1, catalogId);
			ps.setString(2, catalogType);
			ps.setString(3, dependency.id());
			ps.setString(4, dependency.type());
			ps.setString(5, userId);
			ps.setString(6, userType);
			ps.setTimestamp(7, timestamp);
		});
	}

	/*
	 * Reads
	 */

	/**
	 * The direct dependencies of a catalog item
	 *
	 * @param catalogId the engine id or project id
	 * @return each as parent_id, catalog_id and catalog_type
	 */
	public static List<Map<String, Object>> getDependencies(String catalogId) {
		return getDependencies(catalogId, false);
	}

	/**
	 * The dependencies of a catalog item, with theirs when asked. Each item is
	 * visited once, so a cycle, such as an engine and a project that depend on each
	 * other, ends.
	 *
	 * @param catalogId       the engine id or project id
	 * @param subdependencies whether to add the dependencies of its dependencies,
	 *                        all the way down
	 * @return each as parent_id, catalog_id and catalog_type
	 */
	public static List<Map<String, Object>> getDependencies(String catalogId, boolean subdependencies) {
		List<Map<String, Object>> all = new ArrayList<>();
		collect(catalogId, subdependencies, new HashSet<>(), all, SecurityCatalogDependencyUtils::getDirectDependencies);
		return all;
	}

	private static List<Map<String, Object>> getDirectDependencies(String catalogId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CATALOGDEPENDENCIES__CATALOGID", "parent_id"));
		qs.addSelector(new QueryColumnSelector("CATALOGDEPENDENCIES__DEPENDENCYID", "catalog_id"));
		qs.addSelector(new QueryColumnSelector("CATALOGDEPENDENCIES__DEPENDENCYTYPE", "catalog_type"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CATALOGDEPENDENCIES__CATALOGID", "==", catalogId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	private static void collect(String catalogId, boolean subdependencies, Set<String> visited,
			List<Map<String, Object>> all, java.util.function.Function<String, List<Map<String, Object>>> direct) {
		if (!visited.add(catalogId)) {
			return;
		}
		for (Map<String, Object> dependency : direct.apply(catalogId)) {
			all.add(dependency);
			if (subdependencies) {
				collect((String) dependency.get("catalog_id"), true, visited, all, direct);
			}
		}
	}

	/**
	 * Whether a user can view each dependency of a catalog item, all the way down
	 *
	 * @param user      the user
	 * @param catalogId the engine id or project id
	 * @return each dependency id and whether they can view it
	 */
	public static Map<String, Object> validateDependencies(User user, String catalogId) {
		Map<String, Object> canView = new LinkedHashMap<>();
		for (Map<String, Object> dependency : getDependencies(catalogId, true)) {
			String id = (String) dependency.get("catalog_id");
			String type = (String) dependency.get("catalog_type");
			if (canView.containsKey(id)) {
				continue;
			}
			boolean viewable;
			try {
				viewable = userCanView(user, id, type);
			} catch (IllegalArgumentException e) {
				// a type no longer known
				viewable = false;
			}
			canView.put(id, viewable);
		}
		return canView;
	}

	/**
	 * The direct dependencies of a catalog item with their names and settings, as
	 * written to an export
	 *
	 * @param catalogId the engine id or project id
	 * @return each with parent_id, catalog_id, catalog_type, catalog_name,
	 *         catalog_subtype, catalog_date_created, catalog_discoverable and
	 *         catalog_global
	 */
	public static List<Map<String, Object>> getDependencyDetails(String catalogId) {
		return getDependencyDetails(catalogId, false);
	}

	/**
	 * The dependencies of a catalog item with their names and settings, with
	 * theirs when asked
	 *
	 * @param catalogId       the engine id or project id
	 * @param subdependencies whether to add the dependencies of its dependencies
	 * @return each as {@link #getDependencyDetails(String)} gives them
	 */
	public static List<Map<String, Object>> getDependencyDetails(String catalogId, boolean subdependencies) {
		List<Map<String, Object>> all = new ArrayList<>();
		collect(catalogId, subdependencies, new HashSet<>(), all, SecurityCatalogDependencyUtils::getDependencyDetailsQuery);
		return all;
	}

	private static List<Map<String, Object>> getDependencyDetailsQuery(String catalogId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		addDetailSelectors(qs, catalogId);
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Adds the selectors, joins and filter every detail query shares: the
	 * dependency, its type, and its name, subtype, date created, discoverable and
	 * global from ENGINE or PROJECT
	 */
	private static void addDetailSelectors(SelectQueryStruct qs, String catalogId) {
		qs.addSelector(new QueryColumnSelector("CATALOGDEPENDENCIES__CATALOGID", "parent_id"));
		qs.addSelector(new QueryColumnSelector("CATALOGDEPENDENCIES__DEPENDENCYID", "catalog_id"));
		qs.addSelector(new QueryColumnSelector("CATALOGDEPENDENCIES__DEPENDENCYTYPE", "catalog_type"));
		qs.addSelector(QueryIfSelector.makeQueryIfSelector(isProjectDependency(),
				new QueryColumnSelector("PROJECT__PROJECTNAME"), new QueryColumnSelector("ENGINE__ENGINENAME"),
				"catalog_name"));
		qs.addSelector(QueryIfSelector.makeQueryIfSelector(isProjectDependency(),
				new QueryColumnSelector("PROJECT__TYPE"), new QueryColumnSelector("ENGINE__ENGINESUBTYPE"),
				"catalog_subtype"));
		qs.addSelector(QueryIfSelector.makeQueryIfSelector(isProjectDependency(),
				new QueryColumnSelector("PROJECT__DATECREATED"), new QueryColumnSelector("ENGINE__DATECREATED"),
				"catalog_date_created"));
		qs.addSelector(QueryIfSelector.makeQueryIfSelector(isProjectDependency(),
				new QueryColumnSelector("PROJECT__DISCOVERABLE"), new QueryColumnSelector("ENGINE__DISCOVERABLE"),
				"catalog_discoverable"));
		qs.addSelector(QueryIfSelector.makeQueryIfSelector(isProjectDependency(),
				new QueryColumnSelector("PROJECT__GLOBAL"), new QueryColumnSelector("ENGINE__GLOBAL"),
				"catalog_global"));
		qs.addRelation("CATALOGDEPENDENCIES__DEPENDENCYID", "ENGINE__ENGINEID", "left.outer.join");
		qs.addRelation("CATALOGDEPENDENCIES__DEPENDENCYID", "PROJECT__PROJECTID", "left.outer.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CATALOGDEPENDENCIES__CATALOGID", "==", catalogId));
	}

	/**
	 * The dependencies of a catalog item with their details and the user's access
	 * to each, with theirs when asked
	 *
	 * @param catalogId       the engine id or project id
	 * @param user            the user
	 * @param subdependencies whether to add the dependencies of its dependencies,
	 *                        with can_view_dependencies on each
	 * @return each with the details, permission, permission_name, group_permission,
	 *         access_permission, description and tags
	 */
	public static List<Map<String, Object>> getDependencyDetails(String catalogId, User user,
			boolean subdependencies) {
		if (!subdependencies) {
			return getDependencyDetailsWithUserQuery(catalogId, user);
		}
		List<Map<String, Object>> all = new ArrayList<>();
		collect(catalogId, true, new HashSet<>(), all, id -> getDependencyDetailsWithUserQuery(id, user));
		calculateCanViewDependencies(all);
		return all;
	}

	/**
	 * Calculate can_view_dependencies field for each dependency by checking if user
	 * has view access to all subdependencies. Works from leaf nodes up the tree.
	 * Now accounts for both user permissions and group permissions.
	 * 
	 * @param allDependencies List of all dependencies (flat structure)
	 */
	private static void calculateCanViewDependencies(List<Map<String, Object>> allDependencies) {
		// Build a map of engineId -> dependency for quick lookup
		Map<String, Map<String, Object>> dependencyMap = new HashMap<>();
		for (Map<String, Object> dep : allDependencies) {
			String engineId = (String) dep.get("catalog_id");
			dependencyMap.put(engineId, dep);
		}

		// Build a map of parent -> list of children directly from allDependencies
		// Since we already recursively fetched everything, we have all the parent-child
		// relationships
		Map<String, List<String>> childrenMap = new HashMap<>();

		for (Map<String, Object> dep : allDependencies) {
			String parentId = (String) dep.get("parent_id");
			String engineId = (String) dep.get("catalog_id");

			// Add this engine as a child of its parent (skip self-references to prevent
			// infinite loops)
			if (parentId != null && !parentId.equals(engineId)) {
				childrenMap.computeIfAbsent(parentId, k -> new ArrayList<>()).add(engineId);
			}
		}

		// Calculate can_view_dependencies from leaf nodes up
		Map<String, Boolean> canViewCache = new HashMap<>();
		Set<String> visiting = new HashSet<>();
		for (Map<String, Object> dep : allDependencies) {
			String engineId = (String) dep.get("catalog_id");
			boolean canView = calculateCanViewRecursive(engineId, dependencyMap, childrenMap, canViewCache, visiting);
			dep.put("can_view_dependencies", canView);
		}
	}

	/**
	 * Recursively calculate if user can view all subdependencies for a given engine
	 * 
	 * @param engineId      The engine ID to check
	 * @param dependencyMap Map of engineId -> dependency data
	 * @param childrenMap   Map of engineId -> list of child engine IDs
	 * @param canViewCache  Cache to avoid recalculating
	 * @param visiting      Set of engine IDs currently being processed (to detect
	 *                      circular dependencies)
	 * @return true if user has view access to this engine and all its
	 *         subdependencies
	 */
	private static boolean calculateCanViewRecursive(String engineId, Map<String, Map<String, Object>> dependencyMap,
			Map<String, List<String>> childrenMap, Map<String, Boolean> canViewCache, Set<String> visiting) {
		// Check cache first
		if (canViewCache.containsKey(engineId)) {
			return canViewCache.get(engineId);
		}

		// Detect circular dependency - if we're already visiting this node, there's a
		// cycle. Every node in the visiting set has already had its own
		// hasViewPermission
		// checked, so the back-edge doesn't introduce any new permission requirement.
		// Return true to avoid poisoning the result for all nodes in the cycle.
		if (visiting.contains(engineId)) {
			return true;
		}

		Map<String, Object> dependency = dependencyMap.get(engineId);
		if (dependency == null) {
			canViewCache.put(engineId, false);
			return false;
		}

		// Check if user has view permission for this engine
		// Account for both user permissions and group permissions
		boolean hasViewPermission = false;
		Integer permission = (Integer) dependency.get("permission");
		Integer groupPermission = (Integer) dependency.get("group_permission");
		Boolean isGlobal = (Boolean) dependency.get("catalog_global");

		// User has view permission if they have any user permission, group permission,
		// or if engine is global
		hasViewPermission = (permission != null) || (groupPermission != null) || (isGlobal != null && isGlobal);

		// If this is a leaf node (no children), return the permission status
		List<String> children = childrenMap.get(engineId);
		if (children == null || children.isEmpty()) {
			canViewCache.put(engineId, hasViewPermission);
			return hasViewPermission;
		}

		// Mark this node as being visited to detect cycles
		visiting.add(engineId);

		try {
			// If this has children, check all subdependencies
			boolean allChildrenViewable = true;
			for (String childId : children) {
				if (!calculateCanViewRecursive(childId, dependencyMap, childrenMap, canViewCache, visiting)) {
					allChildrenViewable = false;
					break;
				}
			}

			// User can view dependencies if they have permission for this node AND all
			// children are viewable
			boolean result = hasViewPermission && allChildrenViewable;
			canViewCache.put(engineId, result);
			return result;
		} finally {
			// Remove from visiting set after processing
			visiting.remove(engineId);
		}
	}

	/**
	 * Query to get project dependency details with user and group permissions for a
	 * single project
	 * 
	 * @param projectId
	 * @param user
	 * @return
	 */
	private static List<Map<String, Object>> getDependencyDetailsWithUserQuery(String projectId, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = user.getPrimaryLoginToken().getId();
		SelectQueryStruct qs = new SelectQueryStruct();
		addDetailSelectors(qs, projectId);

		// METADATA sub-query - conditional based on type (ENGINEMETA vs PROJECTMETA)
		{
			// ENGINE metadata sub-query for description
			SelectQueryStruct engineMetaQs = new SelectQueryStruct();
			engineMetaQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID", "ENGINEID"));
			engineMetaQs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE", "DESCRIPTION"));
			engineMetaQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", "description"));
			engineMetaQs.addGroupBy(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
			engineMetaQs.addGroupBy(new QueryColumnSelector("ENGINEMETA__METAVALUE"));

			SubqueryRelationship engineMetaRel = new SubqueryRelationship(engineMetaQs, "EM", "left.outer.join",
					new String[] { "EM__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineMetaRel);

			// PROJECT metadata sub-query for description
			SelectQueryStruct projectMetaQs = new SelectQueryStruct();
			projectMetaQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID", "PROJECTID"));
			projectMetaQs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE", "DESCRIPTION"));
			projectMetaQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", "description"));
			projectMetaQs.addGroupBy(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
			projectMetaQs.addGroupBy(new QueryColumnSelector("PROJECTMETA__METAVALUE"));

			SubqueryRelationship projectMetaRel = new SubqueryRelationship(projectMetaQs, "PM", "left.outer.join",
					new String[] { "PM__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectMetaRel);

			// Use conditional selector for description
			QueryIfSelector descriptionSelector = QueryIfSelector.makeQueryIfSelector(
					isProjectDependency(),
					new QueryColumnSelector("PM__DESCRIPTION"), new QueryColumnSelector("EM__DESCRIPTION"),
					"description");
			qs.addSelector(descriptionSelector);

			// ENGINE metadata sub-query for tags - aggregated since there can be multiple
			SelectQueryStruct engineTagsQs = new SelectQueryStruct();
			engineTagsQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID", "ENGINEID"));
			QueryFunctionSelector engineTagsAggregator = new QueryFunctionSelector();
			engineTagsAggregator.setFunction(QueryFunctionHelper.GROUP_CONCAT);
			engineTagsAggregator.addInnerSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
			engineTagsAggregator.setAlias("TAGS");
			engineTagsQs.addSelector(engineTagsAggregator);
			engineTagsQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", "tag"));
			engineTagsQs.addGroupBy(new QueryColumnSelector("ENGINEMETA__ENGINEID"));

			SubqueryRelationship engineTagsRel = new SubqueryRelationship(engineTagsQs, "ET", "left.outer.join",
					new String[] { "ET__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineTagsRel);

			// PROJECT metadata sub-query for tags - aggregated since there can be multiple
			SelectQueryStruct projectTagsQs = new SelectQueryStruct();
			projectTagsQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID", "PROJECTID"));
			QueryFunctionSelector projectTagsAggregator = new QueryFunctionSelector();
			projectTagsAggregator.setFunction(QueryFunctionHelper.GROUP_CONCAT);
			projectTagsAggregator.addInnerSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
			projectTagsAggregator.setAlias("TAGS");
			projectTagsQs.addSelector(projectTagsAggregator);
			projectTagsQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", "tag"));
			projectTagsQs.addGroupBy(new QueryColumnSelector("PROJECTMETA__PROJECTID"));

			SubqueryRelationship projectTagsRel = new SubqueryRelationship(projectTagsQs, "PT", "left.outer.join",
					new String[] { "PT__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectTagsRel);

			// Use conditional selector for tags
			QueryIfSelector tagsSelector = QueryIfSelector.makeQueryIfSelector(
					isProjectDependency(),
					new QueryColumnSelector("PT__TAGS"), new QueryColumnSelector("ET__TAGS"), "tags");
			qs.addSelector(tagsSelector);
		}

		// PERMISSION sub-query - conditional based on type (ENGINEPERMISSION vs
		// PROJECTPERMISSION)
		{
			// ENGINE permissions sub-query
			SelectQueryStruct enginePermQs = new SelectQueryStruct();
			enginePermQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID", "ENGINEID"));
			enginePermQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION", "PERMISSION"));
			enginePermQs.addRelation("ENGINEPERMISSION__PERMISSION", "PERMISSION__ID", "inner.join");
			enginePermQs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "PERMISSION_NAME"));
			enginePermQs
					.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
			enginePermQs.addGroupBy(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			enginePermQs.addGroupBy(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
			enginePermQs.addGroupBy(new QueryColumnSelector("PERMISSION__NAME"));

			SubqueryRelationship enginePermRel = new SubqueryRelationship(enginePermQs, "EP", "left.outer.join",
					new String[] { "EP__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(enginePermRel);

			// PROJECT permissions sub-query
			SelectQueryStruct projectPermQs = new SelectQueryStruct();
			projectPermQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			projectPermQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION", "PERMISSION"));
			projectPermQs.addRelation("PROJECTPERMISSION__PERMISSION", "PERMISSION__ID", "inner.join");
			projectPermQs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "PERMISSION_NAME"));
			projectPermQs
					.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
			projectPermQs.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
			projectPermQs.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
			projectPermQs.addGroupBy(new QueryColumnSelector("PERMISSION__NAME"));

			SubqueryRelationship projectPermRel = new SubqueryRelationship(projectPermQs, "PP", "left.outer.join",
					new String[] { "PP__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectPermRel);

			// Use conditional selectors for permissions
			QueryIfSelector permissionSelector = QueryIfSelector.makeQueryIfSelector(
					isProjectDependency(),
					new QueryColumnSelector("PP__PERMISSION"), new QueryColumnSelector("EP__PERMISSION"), "permission");
			qs.addSelector(permissionSelector);

			QueryIfSelector permissionNameSelector = QueryIfSelector.makeQueryIfSelector(
					isProjectDependency(),
					new QueryColumnSelector("PP__PERMISSION_NAME"), new QueryColumnSelector("EP__PERMISSION_NAME"),
					"permission_name");
			qs.addSelector(permissionNameSelector);
		}

		// GROUP PERMISSION sub-query - conditional based on type (GROUPENGINEPERMISSION
		// vs
		// GROUPPROJECTPERMISSION)
		{
			// Build the group filter once for reuse
			OrQueryFilter groupOrFilters = new OrQueryFilter();
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider login : logins) {
					AccessToken accessToken = user.getAccessToken(login);
					Collection<String> userGroups = accessToken.getUserGroups();
					String userGroupType = accessToken.getUserGroupType();
					Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
					if (!userCustomGroups.isEmpty()) {
						AndQueryFilter customAndFilter = new AndQueryFilter();
						customAndFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", "CUSTOM"));
						customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID",
								"==", userCustomGroups));
						groupOrFilters.addFilter(customAndFilter);
					}
					if (!userGroups.isEmpty()) {
						AndQueryFilter andFilter = new AndQueryFilter();
						andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==",
								userGroupType));
						andFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", userGroups));
						groupOrFilters.addFilter(andFilter);
					}
				}
			}

			// ENGINE group permissions sub-query
			SelectQueryStruct engineGroupPermQs = new SelectQueryStruct();
			engineGroupPermQs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID", "ENGINEID"));
			engineGroupPermQs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					"GROUPENGINEPERMISSION__PERMISSION", "PERMISSION"));
			engineGroupPermQs.addGroupBy(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID"));
			if (!groupOrFilters.isEmpty()) {
				engineGroupPermQs.addExplicitFilter(groupOrFilters);
			} else {
				AndQueryFilter nullFilter = new AndQueryFilter();
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", null));
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", null));
				engineGroupPermQs.addExplicitFilter(nullFilter);
			}

			SubqueryRelationship engineGroupPermRel = new SubqueryRelationship(engineGroupPermQs, "EGP",
					"left.outer.join", new String[] { "EGP__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineGroupPermRel);

			// PROJECT group permissions sub-query
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			if (user != null) {
				List<AuthProvider> logins = user.getLogins();
				for (AuthProvider login : logins) {
					AccessToken accessToken = user.getAccessToken(login);
					Collection<String> userGroups = accessToken.getUserGroups();
					String userGroupType = accessToken.getUserGroupType();
					Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
					if (!userCustomGroups.isEmpty()) {
						AndQueryFilter customAndFilter = new AndQueryFilter();
						customAndFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", "CUSTOM"));
						customAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID",
								"==", userCustomGroups));
						groupProjectOrFilters.addFilter(customAndFilter);
					}
					if (!userGroups.isEmpty()) {
						AndQueryFilter andFilter = new AndQueryFilter();
						andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==",
								userGroupType));
						andFilter.addFilter(
								SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", userGroups));
						groupProjectOrFilters.addFilter(andFilter);
					}
				}
			}

			SelectQueryStruct projectGroupPermQs = new SelectQueryStruct();
			projectGroupPermQs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PROJECTID", "PROJECTID"));
			projectGroupPermQs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					"GROUPPROJECTPERMISSION__PERMISSION", "PERMISSION"));
			projectGroupPermQs.addGroupBy(new QueryColumnSelector("GROUPPROJECTPERMISSION__PROJECTID"));
			if (!groupProjectOrFilters.isEmpty()) {
				projectGroupPermQs.addExplicitFilter(groupProjectOrFilters);
			} else {
				AndQueryFilter nullFilter = new AndQueryFilter();
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", null));
				nullFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", null));
				projectGroupPermQs.addExplicitFilter(nullFilter);
			}

			SubqueryRelationship projectGroupPermRel = new SubqueryRelationship(projectGroupPermQs, "PGP",
					"left.outer.join", new String[] { "PGP__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectGroupPermRel);

			// Use conditional selector for group permission based on engine type
			QueryIfSelector groupPermissionSelector = QueryIfSelector.makeQueryIfSelector(
					isProjectDependency(),
					new QueryColumnSelector("PGP__PERMISSION"), new QueryColumnSelector("EGP__PERMISSION"),
					"group_permission");
			qs.addSelector(groupPermissionSelector);
		}

		// ACCESS REQUEST sub-query - conditional based on type (ENGINEACCESSREQUEST vs
		// PROJECTACCESSREQUEST)
		{
			// ENGINE access requests sub-query
			SelectQueryStruct engineAccReqQs = new SelectQueryStruct();
			engineAccReqQs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__PERMISSION", "PERMISSION"));
			engineAccReqQs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__ENGINEID", "ENGINEID"));
			engineAccReqQs.addRelation("ENGINEACCESSREQUEST__ENGINEID", "ENGINE__ENGINEID", "inner.join");
			engineAccReqQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__REQUEST_USERID", "==", userId));
			engineAccReqQs.addExplicitFilter(SimpleQueryFilter
					.makeColToValFilter("ENGINEACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));

			SubqueryRelationship engineAReqRel = new SubqueryRelationship(engineAccReqQs, "EAR", "left.outer.join",
					new String[] { "EAR__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs.addRelation(engineAReqRel);

			// PROJECT access requests sub-query
			SelectQueryStruct projectAccReqQs = new SelectQueryStruct();
			projectAccReqQs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PERMISSION", "PERMISSION"));
			projectAccReqQs.addSelector(new QueryColumnSelector("PROJECTACCESSREQUEST__PROJECTID", "PROJECTID"));
			projectAccReqQs.addRelation("PROJECTACCESSREQUEST__PROJECTID", "PROJECT__PROJECTID", "inner.join");
			projectAccReqQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("PROJECTACCESSREQUEST__REQUEST_USERID", "==", userId));
			projectAccReqQs.addExplicitFilter(SimpleQueryFilter
					.makeColToValFilter("PROJECTACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));

			SubqueryRelationship projectAReqRel = new SubqueryRelationship(projectAccReqQs, "PAR", "left.outer.join",
					new String[] { "PAR__PROJECTID", "PROJECT__PROJECTID", "=" });
			qs.addRelation(projectAReqRel);

			// Use conditional selector for access permission
			QueryIfSelector accessPermissionSelector = QueryIfSelector.makeQueryIfSelector(
					isProjectDependency(),
					new QueryColumnSelector("PAR__PERMISSION"), new QueryColumnSelector("EAR__PERMISSION"),
					"access_permission");
			qs.addSelector(accessPermissionSelector);
		}

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Gives a person the requested access to the engines and projects a project
	 * depends on, or asks their owners for it. On a model, a person added gets no
	 * usage limit, and a person whose access changes keeps theirs.
	 *
	 * @param requester           the user asking
	 * @param projectId           the project
	 * @param newUserId           the person
	 * @param newUserType         the person's login type
	 * @param requestedPermission the level, by name
	 * @param endDate             when the access ends, or null
	 * @return the engines and projects by what was done for each
	 */
	public static Map<String, Object> propagatePermission(User requester, String projectId, String newUserId,
			String newUserType, String requestedPermission, String endDate) {
		return propagate(requester, projectId, newUserId, newUserType, requestedPermission, endDate, null);
	}

	/**
	 * Gives a person the requested access to the engines and projects a project
	 * depends on, or asks their owners for it. Only a model's owners set a limit
	 * that differs from the current one.
	 *
	 * @param requester           the user asking
	 * @param projectId           the project
	 * @param newUserId           the person
	 * @param newUserType         the person's login type
	 * @param requestedPermission the level, by name
	 * @param endDate             when the access ends, or null
	 * @param usageLimit          the person's model usage limit on each model given
	 *                            or changed, or null for no limit defined
	 * @return the engines and projects by what was done for each
	 */
	public static Map<String, Object> propagatePermission(User requester, String projectId, String newUserId,
			String newUserType, String requestedPermission, String endDate, ModelUsageLimit usageLimit) {
		return propagate(requester, projectId, newUserId, newUserType, requestedPermission, endDate,
				new PermissionGrant().withUsageLimit(usageLimit));
	}

	/**
	 * Whether only admins can give people access to an engine or project a project
	 * depends on. A dependency whose setting cannot be read counts as admin-only.
	 *
	 * @param isEngine whether the dependency is an engine rather than a project
	 * @param id       the dependency's id
	 * @return whether its admin-only add setting is on
	 */
	private static boolean dependencyAddsAreAdminOnly(boolean isEngine, String id) {
		try {
			return isEngine ? adminOnlyEngineAddAccess(id) : adminOnlyProjectAddAccess(id);
		} catch (IllegalArgumentException e) {
			classLogger.warn("Could not read the admin-only add setting of {}", id, e);
			return true;
		}
	}

	/**
	 * Does the work of both propagatePermission methods: for each engine and
	 * project the project depends on, it gives the person the requested level, or
	 * files an access request for its owners when the requester is Read-Only or has
	 * no access there.
	 *
	 * @param limit the person's model usage limit on the models: a grant that gives
	 *              {@code usageLimit}
	 *              ({@link PermissionGrant#isUsageLimitGiven()}), or null for none,
	 *              so a person whose access changes keeps theirs
	 */
	private static Map<String, Object> propagate(User requester, String projectId, String newUserId, String newUserType,
			String requestedPermission, String endDate, PermissionGrant limit) {
		Map<String, Object> ret = new HashMap<String, Object>();

		// get the requested permission as a numeric -- it was passed as a string
		Integer requestedPermissionNumeric = AccessPermissionEnum.getIdByPermission(requestedPermission);
		boolean requesterIsAdmin = SecurityAdminUtils.userIsAdmin(requester);

		List<String> alreadyHaveAccess = new ArrayList<>();
		List<String> requestAlreadyExists = new ArrayList<>();
		List<String> newRequestAdded = new ArrayList<>();
		List<String> accessGranted = new ArrayList<>();
		List<String> couldNotAddRequest = new ArrayList<>();

		// loop through the dependencies and process request according to the
		// requestor's permissions on each engine.
		List<Map<String, Object>> dependentEngines = getDependencies(projectId);
		for (int i = 0; i < dependentEngines.size(); i++) {
			Map<String, Object> dependentEngine = dependentEngines.get(i);
			String dependentEngineId = (String) dependentEngine.get("catalog_id");
			String dependentEngineType = (String) dependentEngine.get("catalog_type");

			Integer currentPendingUserPermission;
			Integer requesterEnginePermission;
			Integer currentNewUserPermission;
			boolean isEngine = false;
			// only a model has usage limits
			PermissionGrant engineLimit = IEngine.CATALOG_TYPE.MODEL.name().equals(dependentEngineType) ? limit : null;

			if (dependentEngineType == null || !CatalogTypes.isProjectType(dependentEngineType)) {
				isEngine = true;
				currentPendingUserPermission = SecurityEngineUtils.getUserAccessRequestEnginePermission(newUserId,
						dependentEngineId);
				requesterEnginePermission = SecurityEngineUtils
						.getUserEnginePermission(User.getSingleLogginName(requester), dependentEngineId);
				currentNewUserPermission = SecurityEngineUtils.getUserEnginePermission(newUserId, dependentEngineId);
			} else {
				currentPendingUserPermission = SecurityProjectUtils.getUserAccessRequestProjectPermission(newUserId,
						dependentEngineId);
				requesterEnginePermission = SecurityProjectUtils
						.getUserProjectPermission(User.getSingleLogginName(requester), dependentEngineId);
				currentNewUserPermission = SecurityProjectUtils.getUserProjectPermission(newUserId, dependentEngineId);
			}

			// if newUser is requesting permission which he/she already has access, take no
			// action
			if (currentNewUserPermission == requestedPermissionNumeric) {
				alreadyHaveAccess.add(dependentEngineId);
				classLogger.info("User already has {} access to {}", requestedPermission, dependentEngineId);
				// if newUser has already requested this access and it is still pending, take no
				// action
			} else if (currentPendingUserPermission != null
					&& requestedPermissionNumeric == currentPendingUserPermission) {
				requestAlreadyExists.add(dependentEngineId);
				classLogger.info("user has already requested {} access to {} and the request is pending.",
						requestedPermission, dependentEngineId);
				// if requester has insufficient privileges on the engine so forward request to
				// engine owner
			} else if (requesterEnginePermission == null || requesterEnginePermission == 3) {
				try {
					if (isEngine) {
						SecurityEngineUtils.setUserAccessRequest(newUserId, newUserType, dependentEngineId,
								"No Comment at this time", requestedPermissionNumeric, requester);
					} else {
						SecurityProjectUtils.setUserAccessRequest(newUserId, newUserType, dependentEngineId,
								"No Comment at this time", requestedPermissionNumeric, requester);
					}
					newRequestAdded.add(dependentEngineId);
					classLogger.info("User has forwarded {}'s request to the owner of engine {}", newUserId,
							dependentEngineId);
				} catch (Exception e) {
					couldNotAddRequest.add(dependentEngineId);
					classLogger.error("Failed to propagate project permissions to related artifacts", e);
				}
				// if the newUser has permissions on the engine but not to the level requested,
				// edit the existing record
			} else if (!requesterIsAdmin && dependencyAddsAreAdminOnly(isEngine, dependentEngineId)) {
				// these grants skip the endpoints, so each dependency's own setting is checked
				couldNotAddRequest.add(dependentEngineId);
				classLogger.info("Only admins can give {} access to {}", newUserId, dependentEngineId);
			} else if (requesterEnginePermission < 3 && currentNewUserPermission != null
					&& currentNewUserPermission > requestedPermissionNumeric) {
				try {
					if (isEngine) {
						editDependentEngineUser(requester, newUserId, newUserType, dependentEngineId,
								requestedPermission, endDate, engineLimit);
					} else {
						SecurityProjectUtils.editProjectUserPermission(requester, newUserId, newUserType,
								dependentEngineId, requestedPermission, endDate);
					}

					accessGranted.add(dependentEngineId);
					classLogger.info("User has updated permission for {} to {}", newUserId, dependentEngineId);
				} catch (IllegalAccessException | IllegalArgumentException e) {
					couldNotAddRequest.add(dependentEngineId);
					classLogger.error("Failed to propagate project permissions to related artifacts", e);
				}
				// if none of the above and requestor has proper permission, add user to the
				// engine permission database
			} else if (requesterEnginePermission < 3 && currentNewUserPermission == null) {
				try {
					if (isEngine) {
						addDependentEngineUser(requester, newUserId, dependentEngineId, requestedPermission, endDate,
								engineLimit);
					} else {
						SecurityProjectUtils.addProjectUser(requester, newUserId, dependentEngineId,
								requestedPermission, endDate);
					}

					accessGranted.add(dependentEngineId);
					classLogger.info("User has added {} to {}", newUserId, dependentEngineId);
				} catch (IllegalAccessException | IllegalArgumentException e) {
					couldNotAddRequest.add(dependentEngineId);
					classLogger.error("Failed to propagate project permissions to related artifacts", e);
				}
			} else {
				couldNotAddRequest.add(dependentEngineId);
				classLogger.info("User could not add or forward {}'s request for engine {}", newUserId,
						dependentEngineId);
			}
		}

		ret.put("Successfully processed permission propagation", true);
		ret.put("alreadyHaveAccess", alreadyHaveAccess);
		ret.put("requestAlreadyExists", requestAlreadyExists);
		ret.put("newRequestAdded", newRequestAdded);
		ret.put("accessGranted", accessGranted);
		ret.put("couldNotAddRequest", couldNotAddRequest);
		return ret;
	}

	/**
	 * Gives a person access to an engine a project depends on, with the usage limit
	 * given, or none.
	 *
	 * @param limit on a model, the limit the propagation gives; null for none
	 */
	private static void addDependentEngineUser(User requester, String newUserId, String engineId, String level,
			String endDate, PermissionGrant limit) throws IllegalAccessException {
		SecurityEngineUtils.addEngineUser(requester, newUserId, engineId, level, endDate,
				limit == null ? null : limit.getUsageLimit());
	}

	/**
	 * Changes a person's access to an engine a project depends on. Their usage
	 * limit is kept unless one is given.
	 *
	 * @param limit on a model, the limit the propagation gives; null for none
	 */
	private static void editDependentEngineUser(User requester, String newUserId, String newUserType, String engineId,
			String level, String endDate, PermissionGrant limit) throws IllegalAccessException {
		if (limit != null && limit.isUsageLimitGiven()) {
			SecurityEngineUtils.editEngineUserPermission(requester, newUserId, newUserType, engineId, level, endDate,
					limit.getUsageLimit());
		} else {
			SecurityEngineUtils.editEngineUserPermission(requester, newUserId, newUserType, engineId, level, endDate);
		}
	}
}
