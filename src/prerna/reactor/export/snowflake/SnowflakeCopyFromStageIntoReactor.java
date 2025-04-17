package prerna.reactor.export.snowflake;

import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class SnowflakeCopyFromStageIntoReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SnowflakePutReactor.class);

	public SnowflakeCopyFromStageIntoReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey(), 
				"userStage", "tableStage", "namedStage"
				};
		this.keyRequired = new int[] {
				1, 1, 
				0, 0, 0
				};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have access to database");
		}

		String tableName = this.keyValue.get(this.keysToGet[1]);
		if(tableName == null || (tableName=tableName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the table we are inserting into");
		}
		String userStage = this.keyValue.get(this.keysToGet[2]);
		String tableStage = this.keyValue.get(this.keysToGet[3]);
		String namedStage = this.keyValue.get(this.keysToGet[4]);
		
		String stageQualifier = "";
		String stageDestination = "";
		if(userStage != null && !(userStage=userStage.trim()).isEmpty()) {
			stageQualifier = "@~";
			stageDestination = userStage;
		} else if(tableStage != null && !(tableStage=tableStage.trim()).isEmpty()) {
			stageQualifier = "@%";
			stageDestination = tableStage;
		} else if(namedStage != null && !(namedStage=namedStage.trim()).isEmpty()) {
			stageQualifier = "@";
			stageDestination = namedStage;
		} else {
			throw new IllegalArgumentException("Must pass in userStage, tableStage, or namedStage. All values were null or empty");
		}

		String sql = "COPY FILES INTO " + tableName + " FROM " +stageQualifier+stageDestination
				;
		
		IDatabaseEngine snowflake = Utility.getDatabase(databaseId);
		IRDBMSEngine snowflakeRdbms = (IRDBMSEngine) snowflake;
		Statement stmt = null;
		try {
			stmt = snowflakeRdbms.getConnection().createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("A SQL exception was thrown. Detailed error = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(snowflakeRdbms, stmt);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "Utility method to execute the COPY INTO command from a file in a stage. Snowflake docs found here: https://docs.snowflake.com/en/sql-reference/sql/copy-files";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The database id for the snowflake db";
		} else if(key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "This is a required value containing the table we are inserting into";
		} else if(key.equals("userStage")) {
			return "The path prefix for the user stage. Do not include the '@~' qualifier";
		} else if(key.equals("tableStage")) {
			return "The table name and path prefix. Do not enter the '@%' qualifier"; 
		} else if(key.equals("namedStage")) {
			return "The named stage and path prefix. This will not create a stage if it does not exist. Do not enter the '@' qualifier "; 
		} 

		return super.getDescriptionForKey(key);
	}

}

