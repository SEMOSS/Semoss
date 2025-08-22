package prerna.io.connector.serviceNow;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ApiPermissionDetails;
import prerna.util.Constants;
import prerna.util.Utility;

public class ServiceNowGetUserAPIPermissionsReactor extends AbstractReactor{
	
	private static final Logger classLogger = LogManager.getLogger(ServiceNowGetUserAPIPermissionsReactor.class);
	private static final String TABLE = "USERAPIPERMISSION";

	@Override
	public NounMetadata execute() {
		ResultSet rs = null;
		HashMap<Object, Object> responseMap = new HashMap<>();
		try {
			IDatabaseEngine database = Utility.getDatabase(Constants.SECURITY_DB);
			String tableName = getTableName(database);
			if (tableName == null) {
				responseMap.put("Data inserted successfully", false);
				responseMap.put("Error", "USERAPIPERMISSION table not found in database.");
				return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
			}
			String query = "SELECT * FROM " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) database.execQuery(query);
			Object rsObj = hashmap.get("RESULTSET_OBJECT");
			List<ApiPermissionDetails> resultList = new ArrayList<>();
			if (rsObj instanceof ResultSet) {
				rs = (ResultSet) rsObj;
				while (rs.next()) {
					ApiPermissionDetails apiPermissionDetails=new ApiPermissionDetails();
					apiPermissionDetails.setApiId(rs.getString("API_ID"));
					apiPermissionDetails.setType(rs.getString("TYPE"));
					apiPermissionDetails.setUserId(rs.getString("USERID"));
					apiPermissionDetails.setUuid(rs.getString("UUID"));
					resultList.add(apiPermissionDetails);
				}
			} else {
				classLogger.error("No ResultSet returned from database query.");
				throw new SemossPixelException(
						NounMetadata.getErrorNounMessage("No ResultSet returned from database query."));
			}
			return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in the reactor JiraGetReactor: " + e.getMessage();
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception ex) {
				classLogger.error("Error closing ResultSet in JiraGetReactor", ex);
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(ex.getMessage()));
			}
		}
	}

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
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(e.getMessage()));
		}
		return null;
	
	}

}
