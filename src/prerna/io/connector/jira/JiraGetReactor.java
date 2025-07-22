package prerna.io.connector.jira;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraDetails;
import prerna.util.Utility;

public class JiraGetReactor extends AbstractReactor {

	public static final String JIRA_UNIQUE_ID = "ID";
	private static final String TABLE = "JIRA_USER";
	private static final Logger classLogger = LogManager.getLogger(JiraGetReactor.class);

	// Centralized method to get table name
	private String getTableName(IDatabaseEngine database) {
		try {
			List<String> tables = database.getPixelConcepts();
			for (String tableName : tables) {
				if (TABLE.equals(tableName)) {
					return tableName;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	@Override
	public NounMetadata execute() {
		ResultSet rs = null;
		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);
			if (tableName == null) {
				String error = "Jira user table not found in database.";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			String query = "SELECT * FROM " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object rsObj = hashmap.get("RESULTSET_OBJECT");
			List<JiraDetails> resultList = new ArrayList<>();

			if (rsObj instanceof ResultSet) {
				rs = (ResultSet) rsObj;
				while (rs.next()) {
					JiraDetails jiraDetails = new JiraDetails();
					jiraDetails.setCreatedBy(rs.getString("CREATED_BY"));
					jiraDetails.setDateCreated(rs.getString("DATE_CREATED"));
					jiraDetails.setDateLastUsed(rs.getString("DATE_LAST_USED"));
					jiraDetails.setPrimaryId(rs.getString(JIRA_UNIQUE_ID));
					jiraDetails.setUrl(rs.getString("URL"));
					jiraDetails.setUserId(rs.getString("USER_ID"));
					jiraDetails.setKeyName(rs.getString("KEY_NAME"));
					resultList.add(jiraDetails);
				}
			} else {
				String error = "No ResultSet returned from database query.";
				return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}

			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in the reactor JiraGetReactor: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception ex) {
				classLogger.error("Error closing ResultSet in JiraGetReactor", ex);
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used for getting a list of all the Jira API related entries in DB to display on UI.";
	}
}
