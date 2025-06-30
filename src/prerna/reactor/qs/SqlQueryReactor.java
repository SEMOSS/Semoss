package prerna.reactor.qs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Unified SQL Query Reactor that:
 * 1. Parses SQL to detect query type (SELECT vs modification)
 * 2. Validates user permissions based on query type
 * 3. Delegates to appropriate existing reactors
 * 
 * Usage: SqlQuery(database=["myDb"], query=["SELECT * FROM table"], limit=[100])
 */
public class SqlQueryReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SqlQueryReactor.class);
	
	
	public SqlQueryReactor() {
		this.keysToGet = new String[]{
			ReactorKeysEnum.QUERY_KEY.getKey(), 
			ReactorKeysEnum.DATABASE.getKey(),
			ReactorKeysEnum.LIMIT.getKey(),
			"commit"
		};
	}

	@Override
	public NounMetadata execute() {
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
		
		User user = this.insight.getUser();
		if (user == null) {
			throw new SemossPixelException("User context is required");
		}
		
		try {
			// determine query type
			QueryType queryType = detectQueryType(sqlQuery);
			classLogger.info("Detected query type: " + queryType + " for user: " + user.getPrimaryLogin());
			

			validateUserPermissions(user, databaseId, queryType);
			
			// create query structure and delegate
			return delegateToAppropriateReactor(sqlQuery, databaseId, queryType, limitStr, commitStr);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing SQL query: " + e.getMessage());
		}
	}
	
	/**
	 * detect SQL statement type using JSQLParser
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
	

	private void validateUserPermissions(User user, String databaseId, QueryType queryType) {
		switch (queryType) {
			case SELECT:
				//view permission
				if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
					throw new SemossPixelException("User does not have permission to query this database");
				}
				break;
			default:
				// any other modification queries need edit permission
				if (!SecurityEngineUtils.userCanEditEngine(user, databaseId)) {
					throw new SemossPixelException("User does not have permission to modify this database");
				}
				break;
		}
	}
	

	private NounMetadata delegateToAppropriateReactor(String sqlQuery, String databaseId, 
			QueryType queryType, String limitStr, String commitStr) {
		
		if (queryType == QueryType.SELECT) {
			return executeSelectQuery(sqlQuery, databaseId, limitStr);
		} else {
			return executeModificationQuery(sqlQuery, databaseId, commitStr);
		}
	}
	
	/**
	 * for select, create task and return it directly, much like the collect reactor
	 */
	private NounMetadata executeSelectQuery(String sqlQuery, String databaseId, String limitStr) {
		try {
			HardSelectQueryStruct qs = getQs(sqlQuery, databaseId);
			
			//set limit if provided
			int limit = 500; // default
			if (limitStr != null && !limitStr.trim().isEmpty()) {
				try {
					limit = Integer.parseInt(limitStr.trim());
				} catch (NumberFormatException e) {
					classLogger.warn("Invalid limit value: " + limitStr + ", using default 500");
				}
			}
			
			BasicIteratorTask task = new BasicIteratorTask(qs);
			task.setNumCollect(limit);
			
			this.insight.addQueriedDatabasesese(databaseId);
			
			return new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing SELECT query: " + e.getMessage());
		}
	}
	
	private NounMetadata executeModificationQuery(String sqlQuery, String databaseId, String commitStr) {
		try {
			HardSelectQueryStruct qs = getQs(sqlQuery, databaseId);
			
			// Add query struct to noun
			GenRowStruct qsGrs = this.store.makeNoun(ReactorKeysEnum.QUERY_STRUCT.getKey());
			qsGrs.add(new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
			
			return delegateToExecQueryReactor(commitStr);
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing modification query: " + e.getMessage());
		}
	}

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
	

	private NounMetadata delegateToExecQueryReactor(String commitStr) {
		try {
			boolean commit = commitStr == null || commitStr.trim().isEmpty() || Boolean.parseBoolean(commitStr);

			GenRowStruct commitGrs = this.store.makeNoun("commit");
			commitGrs.add(new NounMetadata(commit, PixelDataType.BOOLEAN));
			
			ExecQueryReactor execReactor = new ExecQueryReactor();
			execReactor.setInsight(this.insight);
			execReactor.setNounStore(this.store);
			
			return execReactor.execute();
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error executing modification query: " + e.getMessage());
		}
	}

	private enum QueryType {
		SELECT,
		INSERT, 
		UPDATE,
		DELETE,
		OTHER  // CREATE, ALTER, DROP, etc.
	}
}