package prerna.io.connector.google;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.model.SpreadSheetDetail;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetGoogleProfileReactor extends AbstractReactor{

	public static final String JIRA_DATABASE="6abf12ab-ae96-4edd-a1af-b56b9a37634d";
	public static final String JIRA_UNIQUE_ID="JIRAPROFILE_UNIQUE_ROW_ID";
	
	private static final Logger classLogger = LogManager.getLogger(GetGoogleProfileReactor.class);
	@Override
	public NounMetadata execute() {
		try {
			String tableName = null;
			List<SpreadSheetDetail> resultList = new ArrayList<SpreadSheetDetail>();
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
					SpreadSheetDetail sheetDetail=new SpreadSheetDetail();
					sheetDetail.setCreatedAt(rs.getString("created_at"));
					sheetDetail.setId(rs.getString("id"));
					sheetDetail.setName(rs.getString("name"));
					sheetDetail.setSpreadSheetId(rs.getString("spreadsheet_id"));
					sheetDetail.setUpdatedAt(rs.getString("update_at"));
					resultList.add(sheetDetail);
				}
			}return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in the reactor JiraGetReactor: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
		
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is user for getting list of all the details of users added to access google spreadsheet";
	}

}
