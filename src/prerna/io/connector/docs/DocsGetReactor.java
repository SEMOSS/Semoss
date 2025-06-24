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

	@Override
	public NounMetadata execute() {
		try {
			String tableName = null;
			List<DocsDetails> resultList = new ArrayList<DocsDetails>();
			DocsDetails docsDetails = new DocsDetails();
			IDatabaseEngine database = Utility.getDatabase("9be6565f-550f-4be0-8758-c25232973cb1");
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String query = "select servicejson from " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				ResultSet rs = (ResultSet) string;
				while (rs.next()) {
					docsDetails.setJson(rs.getString("ServiceJson"));
					resultList.add(docsDetails);
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
