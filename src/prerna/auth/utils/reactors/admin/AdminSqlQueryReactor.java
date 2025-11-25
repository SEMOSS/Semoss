package prerna.auth.utils.reactors.admin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.reactor.qs.ExecQueryReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Admin version of SqlQueryReactor.
 *
 * This reactor:
 * Only checks whether the user is currently in admin mode.
 * Allows all SQL types (SELECT + INSERT/UPDATE/DELETE + CREATE/ALTER/DROP).
 */
public class AdminSqlQueryReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(AdminSqlQueryReactor.class);

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 5_000;

    private enum QueryType {
        SELECT, INSERT, UPDATE, DELETE, OTHER
    }

    public AdminSqlQueryReactor() {
    	this.keysToGet = new String[] { ReactorKeysEnum.QUERY_KEY.getKey(), ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), "commit" };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0 };
	}


    @Override
    public NounMetadata execute() {
    	User user = this.insight.getUser();
    	SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		organizeKeys();
		String sqlQuery = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[0]));
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		String limitStr = this.keyValue.get(this.keysToGet[2]);
		String commitStr = this.keyValue.get(this.keysToGet[3]);

		if (sqlQuery == null || sqlQuery.trim().isEmpty()) {
			throw new SemossPixelException("SQL query cannot be empty");
		}

		if (databaseId == null || databaseId.trim().isEmpty()) {
			throw new SemossPixelException("Database ID is required");
		}

        try {
            QueryType queryType = detectQueryType(sqlQuery);
            if(adminUtils == null && queryType != QueryType.SELECT ) {
    			throw new IllegalArgumentException("User must be an admin to perform this function");
    		}
            classLogger.info("Admin SQL Query type detected: {}", queryType);

            return delegateToAppropriateReactor(sqlQuery, databaseId, queryType, limitStr, commitStr);

        } catch (Exception e) {
            classLogger.error(Constants.STACKTRACE, e);
            throw new SemossPixelException("Error executing admin SQL query: " + e.getMessage());
        }
    }

    /**
	 * Detect SQL statement type using JSQLParser
	 */
	private QueryType detectQueryType(String sql) {
		try {
			Statement statement = CCJSqlParserUtil.parse(sql);

			if (statement instanceof Select) {
				return QueryType.SELECT;
			} else if (statement instanceof Insert) {
				return QueryType.INSERT;
			} else if (statement instanceof Update) {
				return QueryType.UPDATE;
			} else if (statement instanceof Delete) {
				return QueryType.DELETE;
			} else {
				// For CREATE, ALTER, DROP, etc.
				return QueryType.OTHER;
			}
		} catch (Exception e) {
			classLogger.warn("Could not parse SQL statement, defaulting to OTHER type: " + e.getMessage());
			return QueryType.OTHER;
		}
	}

	/**
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @param queryType
	 * @param limitStr
	 * @param commitStr
	 * @return
	 */
	private NounMetadata delegateToAppropriateReactor(String sqlQuery, String databaseId, QueryType queryType,
			String limitStr, String commitStr) {

		if (queryType == QueryType.SELECT) {
			return executeSelectQuery(sqlQuery, databaseId, limitStr);
		} else {
			return executeModificationQuery(sqlQuery, databaseId, commitStr);
		}
	}

	/**
	 * For select, create task and return it directly, much like the collect reactor
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @param limitStr
	 * @return
	 */
	private NounMetadata executeSelectQuery(String sqlQuery, String databaseId, String limitStr) {
		try {
			HardSelectQueryStruct qs = getQs(sqlQuery, databaseId);
			// set limit if provided
			int limit = parseLimit(limitStr);
			BasicIteratorTask task = new BasicIteratorTask(qs);
			task.setNumCollect(limit);
			this.insight.addQueriedDatabasesese(databaseId);
			return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing SELECT query: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @param commitStr
	 * @return
	 */
	private NounMetadata executeModificationQuery(String sqlQuery, String databaseId, String commitStr) {
		try {
			HardSelectQueryStruct qs = getQs(sqlQuery, databaseId);
			// default to false if not provided
			boolean commit = commitStr != null && !commitStr.trim().isEmpty() && Boolean.parseBoolean(commitStr.trim());

			NounStore execQueryNounStore = new NounStore("ExecQuery");
			execQueryNounStore.makeGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())
					.add(new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
			execQueryNounStore.makeGenRowStruct("commit").add(new NounMetadata(commit, PixelDataType.BOOLEAN));

			ExecQueryReactor execReactor = new ExecQueryReactor();
			execReactor.setInsight(this.insight);
			execReactor.setNounStore(execQueryNounStore);

			return execReactor.execute();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing modification query: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param sqlQuery
	 * @param databaseId
	 * @return
	 */
	private HardSelectQueryStruct getQs(String sqlQuery, String databaseId) {
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if (engine == null) {
			throw new SemossPixelException("Database with ID '" + databaseId + "' not found or could not be loaded");
		}

		// Create HardSelectQueryStruct for raw SQL
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setEngineId(databaseId);
		qs.setEngine(engine);
		qs.setQuery(sqlQuery);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return qs;
	}

	/**
	 * Parse and validate limit parameter
	 * 
	 * @param limitStr
	 * @return
	 */
	private int parseLimit(String limitStr) {
		int limit = DEFAULT_LIMIT; // default

		if (limitStr != null && !limitStr.trim().isEmpty()) {
			try {
				limit = Integer.parseInt(limitStr.trim());

				if (limit <= 0) {
					classLogger.warn("Non-positive limit value: " + limit + ", using default " + DEFAULT_LIMIT);
					limit = DEFAULT_LIMIT;
				} else if (limit > MAX_LIMIT) {
					classLogger.warn("Limit value " + limit + " exceeds maximum " + MAX_LIMIT + ", using maximum");
					limit = MAX_LIMIT;
				}

			} catch (NumberFormatException e) {
				classLogger.warn("Invalid limit value: " + limitStr + ", using default " + DEFAULT_LIMIT);
			}
		}

		return limit;
	}

	@Override
	public String getReactorDescription() {
		return "Admin-only SQL query execution reactor, bypassing normal permission checks.";
	}

}