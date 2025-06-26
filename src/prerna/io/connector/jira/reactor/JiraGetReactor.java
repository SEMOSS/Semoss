package prerna.io.connector.jira.reactor;

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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.JiraDetails;
import prerna.util.JiraHelper;
import prerna.util.Utility;

public class JiraGetReactor extends AbstractReactor {
	
	public static final String JIRA_DATABASE="bcdb0a92-2a3b-4c73-bb79-5f5116bd6832";
	public static final String JIRA_UNIQUE_ID="JIRAPROFILE_UNIQUE_ROW_ID";
	
	private static final Logger classLogger = LogManager.getLogger(JiraGetReactor.class);

	@Override
	public NounMetadata execute() {
		try {
			String tableName = null;
			List<JiraDetails> resultList = new ArrayList<JiraDetails>();
			IDatabaseEngine database = Utility.getDatabase(JIRA_DATABASE);
			List<String> tables = database.getPixelConcepts();
			for (String element : tables) {
				tableName = element;
			}
			String query = "select * from " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					JiraDetails jiraDetails = new JiraDetails();
					jiraDetails.setCreatedBy(rs.getString("CREATED_BY"));
					jiraDetails.setDateCreated(rs.getString("DATE_CREATED"));
					jiraDetails.setDateLastUsed(rs.getString("DATE_LAST_USED"));
					jiraDetails.setName(rs.getString("NAME"));
					jiraDetails.setPrimaryId(rs.getString(JIRA_UNIQUE_ID));
					jiraDetails.setUrl(rs.getString("URL"));
					jiraDetails.setUserId(rs.getString("USER_ID"));
					jiraDetails.setKeyName(rs.getString("KEY_NAME"));
					resultList.add(jiraDetails);
				}
			}
			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in the reactor JiraGetReactor: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is user for getting list of all the Jira API related enteries in DB to display on UI.";
	}
}
