package prerna.io.connector.docs;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.reactor.AbstractReactor;
import prerna.util.Utility;

public class DocsGetReactor extends AbstractReactor {

	private static final String EngineId = "26a0d483-a005-4885-8420-46c685c5ee52";

	@Override
	public NounMetadata execute() {
		try {
			this.organizeKeys();
			String tableName = null;
			String ResultSetObj = "RESULTSET_OBJECT";
			List<DocsDetails> resultList = new ArrayList<DocsDetails>();
			IDatabaseEngine database = Utility.getDatabase(EngineId);
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String query = "select * from " + tableName;
			@SuppressWarnings("unchecked")
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get(ResultSetObj);
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					DocsDetails docsDetails = new DocsDetails();
					docsDetails.setName(rs.getString("name"));
					docsDetails.setDocName(rs.getString("docname"));
					docsDetails.setDateCreated(rs.getTimestamp("datecreated"));
					docsDetails.setLastUpdatedDate(rs.getTimestamp("lastupdateddate"));
					docsDetails.setUserEmail(rs.getString("insight_usermailid"));
					resultList.add(docsDetails);
				}
			}
			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			String error = "Error in the reactor JiraGetReactor: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}
	}

}
