package prerna.io.connector.docs;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GoogleDocsDeleteReactor extends AbstractReactor {
	private static final String EngineId = "26a0d483-a005-4885-8420-46c685c5ee52";

	public GoogleDocsDeleteReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String name = this.keyValue.get(this.keysToGet[0]);
		int id = getID(name);
		String deletetableName = null;
		try {
			IDatabaseEngine database = Utility.getDatabase(EngineId);
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				deletetableName = table;
			}
			String deletequery = "DELETE FROM " + deletetableName + " WHERE id = " + id;
			database.removeData(deletequery);
			String message = "Row with id " + id + " succesfully deleted";
			return new NounMetadata(message, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			
		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in getting the user ids";
			return new NounMetadata(error + ":" + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}
	
	public static int getID(String name) {
		String tableName = null;
		try {
			String ResultSetObj = "RESULTSET_OBJECT";
			IDatabaseEngine database = Utility.getDatabase(EngineId);
			List<String> tableNames = database.getPixelConcepts();
			for (String table : tableNames) {
				tableName = table;
			}
			String query = "select id from " + tableName + " where name = '" + name + "'";
			@SuppressWarnings("unchecked")
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object rsObj = hashmap.get(ResultSetObj);

			if (rsObj instanceof ResultSet) {
				ResultSet rs = (ResultSet) rsObj;
				if (rs.next()) {
					return rs.getInt("id");
				}
			}
			throw new Exception("Service Account not found");
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor delete the row from the googledocsprofile.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Id" + ReactorKeysEnum.ID.getKey();
		}
		return super.getDescriptionForKey(key);
	}

}
