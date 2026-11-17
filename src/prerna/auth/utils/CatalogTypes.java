package prerna.auth.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.project.api.IProject;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * The types a catalog item, an engine or a project, is kept with in
 * CATALOGDEPENDENCIES: its own engine type (FUNCTION, MODEL, DATABASE, VECTOR,
 * STORAGE, GUARDRAIL) or project type (CODE, BLOCKS, INSIGHTS, SKILL, NOTEBOOK,
 * AUTOMATION, WORKSPACE). The engine and project type names do not overlap, so
 * a type alone says whether the item is an engine or a project.
 */
public final class CatalogTypes {

	/** Whether a catalog item is an engine or a project */
	public enum Kind {
		ENGINE, PROJECT
	}

	/** The engine types an item can have, without the deprecated VENV */
	private static final List<String> ENGINE_TYPES;
	/** The project types an item can have */
	private static final List<String> PROJECT_TYPES;

	static {
		List<String> engineTypes = new ArrayList<>();
		for (IEngine.CATALOG_TYPE type : IEngine.CATALOG_TYPE.values()) {
			if (type != IEngine.CATALOG_TYPE.PROJECT && type != IEngine.CATALOG_TYPE.VENV) {
				engineTypes.add(type.name());
			}
		}
		ENGINE_TYPES = Collections.unmodifiableList(engineTypes);
		List<String> projectTypes = new ArrayList<>();
		for (IProject.PROJECT_TYPE type : IProject.PROJECT_TYPE.values()) {
			projectTypes.add(type.name());
		}
		PROJECT_TYPES = Collections.unmodifiableList(projectTypes);
	}

	private CatalogTypes() {

	}

	/**
	 * @return every engine type a catalog item can have, without VENV
	 */
	public static List<String> engineTypes() {
		return ENGINE_TYPES;
	}

	/**
	 * @return every project type a catalog item can have
	 */
	public static List<String> projectTypes() {
		return PROJECT_TYPES;
	}

	/**
	 * Reads a type as it is kept: upper case and trimmed, and INSIGHT, as the UI
	 * calls it, as INSIGHTS.
	 *
	 * @param type an engine type or a project type, in any case
	 * @return the type as it is kept
	 * @throws IllegalArgumentException when the type is none of them
	 */
	public static String normalize(String type) {
		if (type == null || type.isBlank()) {
			throw new IllegalArgumentException("A catalog type is required");
		}
		String normalized = type.trim().toUpperCase(Locale.ROOT);
		if ("INSIGHT".equals(normalized)) {
			normalized = IProject.PROJECT_TYPE.INSIGHTS.name();
		}
		if (!ENGINE_TYPES.contains(normalized) && !PROJECT_TYPES.contains(normalized)) {
			throw new IllegalArgumentException("Unknown catalog type '" + type + "'. Use one of " + ENGINE_TYPES
					+ " or " + PROJECT_TYPES);
		}
		return normalized;
	}

	/**
	 * Whether a type is an engine type or a project type.
	 *
	 * @param type an engine type or a project type, in any case
	 * @return its kind
	 * @throws IllegalArgumentException when the type is none of them
	 */
	public static Kind kindOf(String type) {
		return PROJECT_TYPES.contains(normalize(type)) ? Kind.PROJECT : Kind.ENGINE;
	}

	/**
	 * Whether a type is a project type
	 *
	 * @param type an engine type or a project type, in any case
	 * @return true for a project type
	 */
	public static boolean isProjectType(String type) {
		return kindOf(type) == Kind.PROJECT;
	}

	/**
	 * A project's type as it is kept, from PROJECT.TYPE. A project with no type, or
	 * one that is not a project type, such as a legacy value, is an INSIGHTS
	 * project, as {@link IProject.PROJECT_TYPE#fromSmss} reads one with none.
	 *
	 * @param projectType the PROJECT.TYPE value
	 * @return the project type
	 */
	public static String projectTypeOf(String projectType) {
		if (projectType != null && !projectType.isBlank()) {
			String normalized = projectType.trim().toUpperCase(Locale.ROOT);
			if ("INSIGHT".equals(normalized)) {
				return IProject.PROJECT_TYPE.INSIGHTS.name();
			}
			if (PROJECT_TYPES.contains(normalized)) {
				return normalized;
			}
		}
		return IProject.PROJECT_TYPE.INSIGHTS.name();
	}

	/**
	 * Finds the type of an engine or project from the security database alone. It
	 * never loads the engine or project, or pulls it from cloud storage, so it is
	 * safe on a cluster where the item is not loaded on this server.
	 *
	 * @param id an engine id or a project id
	 * @return its engine type or project type, or null when the security database
	 *         has neither. A project with no type is an INSIGHTS project.
	 */
	public static String resolve(String id) {
		if (id == null || id.isBlank()) {
			return null;
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			String engineType = QueryExecutionUtility.queryOne(securityDb,
					"SELECT ENGINETYPE FROM ENGINE WHERE ENGINEID = ?", ps -> ps.setString(1, id),
					rs -> rs.getString(1));
			if (engineType != null) {
				return normalize(engineType);
			}
			String[] project = QueryExecutionUtility.queryOne(securityDb,
					"SELECT PROJECTID, TYPE FROM PROJECT WHERE PROJECTID = ?", ps -> ps.setString(1, id),
					rs -> new String[] { rs.getString(1), rs.getString(2) });
			if (project != null) {
				return projectTypeOf(project[1]);
			}
			return null;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the catalog type of " + id, e);
		}
	}
}
