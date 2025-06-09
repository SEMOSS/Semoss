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

public class JiraGetReactor extends AbstractReactor{

	@Override
	public NounMetadata execute() {
		try {
			String tableName = null;
			List<JiraDetails> resultList = new ArrayList<JiraDetails>();
			JiraDetails jiraDetails = new JiraDetails();
			IDatabaseEngine database = Utility.getDatabase("c44b138d-aa8e-42cc-a925-6c2ac855df64");
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
					jiraDetails.setApiKey(rs.getString("APIKEY"));
					jiraDetails.setUserId(rs.getString("USERID"));
					jiraDetails.setJiraPrimaryId(rs.getString("JIRAPROFILE_UNIQUE_ROW_ID"));
					jiraDetails.setUrl(rs.getString("URL"));
					jiraDetails.setDateCreated(rs.getString("DATE_CREATED"));
					jiraDetails.setLastUsed(rs.getString("LAST_USED"));
					resultList.add(jiraDetails);
				}
			}
			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			String error="Error in the reactor JiraGetReactor: "+e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

}
