package prerna.auth.utils;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Moves the rows of PROJECTDEPENDENCIES, where only a project could own
 * dependencies, into CATALOGDEPENDENCIES at start up, then drops
 * PROJECTDEPENDENCIES.
 *
 * <p>
 * Each row is copied with the specific type of its owner (the project's type)
 * and of its dependency (the engine's type, or the project's type), read from
 * ENGINE and PROJECT. That also replaces the generic PROJECT type and fills the
 * missing ones of older rows. A row whose owner or dependency is in neither
 * table any more is logged and left out.
 * </p>
 *
 * <p>
 * Several servers can start on one security database at the same time, as on a
 * cluster, so the move is safe to run at once and to run again:
 * </p>
 * <ul>
 * <li>The copy and the drop are one transaction that starts by locking the rows
 * of PROJECTDEPENDENCIES, with an update that changes nothing. Another server's
 * move waits on those rows until the first commits.</li>
 * <li>A dependency already in CATALOGDEPENDENCIES is not copied again, so a
 * second move adds nothing.</li>
 * <li>A move that fails is tried again, and one that finds PROJECTDEPENDENCIES
 * gone, dropped by another server meanwhile, is done.</li>
 * </ul>
 *
 * @deprecated Only security databases from before CATALOGDEPENDENCIES have
 *             PROJECTDEPENDENCIES. Remove this class, and its call in
 *             {@link AbstractSecurityUtils}, once every security database has
 *             started on a release that moves it.
 */
@Deprecated
final class CatalogDependenciesMigration {

	private static final Logger classLogger = LogManager.getLogger(CatalogDependenciesMigration.class);

	/** The table the dependencies are kept in */
	static final String NEW_TABLE = "CATALOGDEPENDENCIES";
	/** The table from before an engine could own dependencies */
	@Deprecated
	static final String OLD_TABLE = "PROJECTDEPENDENCIES";

	/**
	 * Each old row with the types of its owner and its dependency: the owner's
	 * project type, and the dependency's engine type, project type, or null when
	 * it is in neither table
	 */
	private static final String SELECT_OLD_ROWS = "SELECT D.PROJECTID, OWNER.TYPE, D.ENGINEID, "
			+ "TARGETENGINE.ENGINETYPE, TARGETPROJECT.PROJECTID, TARGETPROJECT.TYPE, D.USERID, D.TYPE, D.DATEADDED "
			+ "FROM " + OLD_TABLE + " D " + "INNER JOIN PROJECT OWNER ON OWNER.PROJECTID = D.PROJECTID "
			+ "LEFT OUTER JOIN ENGINE TARGETENGINE ON TARGETENGINE.ENGINEID = D.ENGINEID "
			+ "LEFT OUTER JOIN PROJECT TARGETPROJECT ON TARGETPROJECT.PROJECTID = D.ENGINEID";
	private static final String SELECT_NEW_KEYS = "SELECT CATALOGID, DEPENDENCYID FROM " + NEW_TABLE;
	private static final String INSERT_NEW_ROW = "INSERT INTO " + NEW_TABLE
			+ " (CATALOGID, CATALOGTYPE, DEPENDENCYID, DEPENDENCYTYPE, USERID, TYPE, DATEADDED) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?)";
	/** How many times a move is tried */
	private static final int ATTEMPTS = 3;
	/** How long to wait before trying a move again, in milliseconds */
	private static final long RETRY_WAIT_MS = 2000;

	private CatalogDependenciesMigration() {

	}

	/**
	 * Moves the rows of PROJECTDEPENDENCIES into CATALOGDEPENDENCIES and drops it,
	 * when the security database still has it. A move that cannot finish is
	 * logged, keeps PROJECTDEPENDENCIES, and runs again on the next start.
	 *
	 * @param securityDb the security database
	 */
	@Deprecated
	static void migrate(IRDBMSEngine securityDb) {
		if (!oldTableExists(securityDb)) {
			return;
		}
		for (int attempt = 1;; attempt++) {
			try {
				int[] counts = QueryExecutionUtility.write(securityDb,
						connection -> move(connection, securityDb.getQueryUtil()));
				classLogger.info("Moved {} dependencies from {} to {} and left out {} rows, then dropped {}",
						counts[0], OLD_TABLE, NEW_TABLE, counts[1], OLD_TABLE);
				return;
			} catch (Exception e) {
				if (!oldTableExists(securityDb)) {
					classLogger.info("{} was moved by another server starting at the same time", OLD_TABLE);
					return;
				}
				if (attempt >= ATTEMPTS) {
					classLogger.error("Unable to move " + OLD_TABLE + " to " + NEW_TABLE + ". " + OLD_TABLE
							+ " is kept, and the move runs again on the next start.", e);
					return;
				}
				classLogger.warn("Unable to move {}, such as while another server starting at the same time "
						+ "moves it. Trying again.", OLD_TABLE, e);
				try {
					Thread.sleep(RETRY_WAIT_MS);
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
	}

	/**
	 * The move, in the caller's transaction
	 *
	 * @return how many dependencies were copied and how many rows left out
	 */
	private static int[] move(Connection connection, AbstractSqlQueryUtil queryUtil) throws Exception {
		// 1. lock the rows to move, so another server's move waits for this one
		QueryExecutionUtility.executeUpdate(connection, "UPDATE " + OLD_TABLE + " SET TYPE = TYPE",
				QueryExecutionUtility.NO_PARAMETERS);

		// 2. copy each row that is not there already, with the specific types
		Set<String> existing = new HashSet<>(QueryExecutionUtility.queryList(connection, SELECT_NEW_KEYS,
				QueryExecutionUtility.NO_PARAMETERS, rs -> key(rs.getString(1), rs.getString(2))));
		List<Object[]> rows = QueryExecutionUtility.queryList(connection, SELECT_OLD_ROWS,
				QueryExecutionUtility.NO_PARAMETERS,
				rs -> new Object[] { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
						rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getTimestamp(9) });
		int oldRowCount = QueryExecutionUtility.queryOne(connection, "SELECT COUNT(*) FROM " + OLD_TABLE,
				QueryExecutionUtility.NO_PARAMETERS, rs -> rs.getInt(1));
		List<Object[]> toInsert = new ArrayList<>();
		for (Object[] row : rows) {
			String ownerId = (String) row[0];
			String dependencyId = (String) row[2];
			String dependencyType = dependencyTypeOf((String) row[3], (String) row[4], (String) row[5]);
			String ownerType = typeOrNull(() -> projectTypeOf((String) row[1]));
			if (dependencyId == null || dependencyType == null || ownerType == null) {
				classLogger.warn("Left out the dependency of project {} on {}: it is not an engine or a project",
						ownerId, dependencyId);
				continue;
			}
			if (ownerId.equals(dependencyId) || !existing.add(key(ownerId, dependencyId))) {
				continue;
			}
			toInsert.add(new Object[] { ownerId, ownerType, dependencyId, dependencyType, row[6],
					row[7], row[8] });
		}
		if (!toInsert.isEmpty()) {
			QueryExecutionUtility.executeBatch(connection, INSERT_NEW_ROW, toInsert, (ps, row) -> {
				for (int i = 0; i < 6; i++) {
					ps.setString(i + 1, (String) row[i]);
				}
				ps.setTimestamp(7, (Timestamp) row[6]);
			});
		}

		// 3. drop the old table in the same transaction
		QueryExecutionUtility.executeUpdate(connection, queryUtil.dropTable(OLD_TABLE),
				QueryExecutionUtility.NO_PARAMETERS);
		return new int[] { toInsert.size(), oldRowCount - toInsert.size() };
	}

	/**
	 * The type of a dependency: the engine's type, or else the project's type
	 *
	 * @return the type, or null when the dependency is in neither table
	 */
	private static String dependencyTypeOf(String engineType, String projectId, String projectType) {
		if (engineType != null) {
			return typeOrNull(() -> CatalogTypes.normalize(engineType));
		}
		if (projectId != null) {
			return typeOrNull(() -> projectTypeOf(projectType));
		}
		return null;
	}

	/**
	 * A type, or null when it is not one a catalog item can have, such as the
	 * deprecated VENV, so its row is left out rather than failing the move
	 */
	private static String typeOrNull(Supplier<String> type) {
		try {
			return type.get();
		} catch (IllegalArgumentException e) {
			classLogger.warn("Left out a dependency with an unknown type: {}", e.getMessage());
			return null;
		}
	}

	/** A project's type, where none or an unknown one is an INSIGHTS project */
	private static String projectTypeOf(String projectType) {
		return CatalogTypes.projectTypeOf(projectType);
	}

	private static String key(String ownerId, String dependencyId) {
		return ownerId + "\u0000" + dependencyId;
	}

	/**
	 * Whether the security database still has PROJECTDEPENDENCIES
	 *
	 * @return true when it has, or when that cannot be read, so nothing is taken
	 *         as done that may not be
	 */
	private static boolean oldTableExists(IRDBMSEngine securityDb) {
		try {
			return securityDb.getQueryUtil().tableExists(securityDb, OLD_TABLE, securityDb.getDatabase(),
					securityDb.getSchema());
		} catch (Exception e) {
			classLogger.warn("Unable to read whether {} exists", OLD_TABLE, e);
			return true;
		}
	}
}
