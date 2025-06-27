package prerna.io.connector.docs;

import java.sql.ResultSet;
import java.util.*;

import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetdocsIdReactor extends AbstractReactor {
	
	private static final String EngineId = "26a0d483-a005-4885-8420-46c685c5ee52";

	public GetdocsIdReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[0]);
		String tableName = null;
		String ID = "id";
		String DOCID = "docid";
		String ResultSetObj = "RESULTSET_OBJECT";
		List<String> docids = new ArrayList<>();
		try {
			IDatabaseEngine database = Utility.getDatabase(EngineId);
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String query = " select docid from " + tableName + " where id= " + id;
			@SuppressWarnings("unchecked")
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object rsObj = hashmap.get(ResultSetObj);

			if (rsObj instanceof ResultSet) {
				ResultSet rs = (ResultSet) rsObj;
				while (rs.next()) {
					docids.add(rs.getString("docid"));
				}
			}
			HashMap<String, Object> res = new HashMap<>();
			res.put(ID, id);
			res.put(DOCID, docids);
			return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);

		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in getting the user ids";
			return new NounMetadata(error + ":" + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}

	}

	@Override
	public String getReactorDescription() {
		return "This reactor returns the document id.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Id" + ReactorKeysEnum.ID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
