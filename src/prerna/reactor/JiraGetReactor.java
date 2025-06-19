package prerna.reactor;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.JiraDetails;
import prerna.util.Utility;

public class JiraGetReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		try {
			String tableName = null;
			List<JiraDetails> resultList = new ArrayList<JiraDetails>();
			IDatabaseEngine database = Utility.getDatabase("3c6f0856-25f0-4bf2-83ae-c4b6253e8b01");
			List<String> pixelConcepts = database.getPixelConcepts();
			for (String element : pixelConcepts) {
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
					jiraDetails.setPrimaryId(rs.getString("JIRAPROFILE_UNIQUE_ROW_ID"));
					jiraDetails.setUrl(rs.getString("URL"));
					jiraDetails.setUserId(rs.getString("USER_ID"));
					resultList.add(jiraDetails);
				}
			}
			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			String error = "Error in the reactor JiraGetReactor: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
	}

}
