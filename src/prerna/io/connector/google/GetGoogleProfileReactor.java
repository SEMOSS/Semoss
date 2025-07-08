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

	private static final String table="Google_USERDB";
	private static final Logger classLogger = LogManager.getLogger(GetGoogleProfileReactor.class);
	
	@Override
	public NounMetadata execute() {
		ResultSet rs =null;
		try {
			String tableName = null;
			List<SpreadSheetDetail> resultList = new ArrayList<SpreadSheetDetail>();
			IDatabaseEngine securityDb = Utility.getDatabase(Constants.SECURITY_DB);
			List<String> tables = securityDb.getPixelConcepts();
			for(String tbl:tables) {
				if(table.equals(tbl)) {
					tableName=tbl;
					break;
				}
			}
			String query = "select * from " + tableName;
			HashMap<String, String> hashmap = (HashMap<String, String>) securityDb.execQuery(query);
			Object string = hashmap.get("RESULTSET_OBJECT");
			if (string instanceof ResultSet) {
				rs = (ResultSet) string;
				while (rs.next()) {
					SpreadSheetDetail sheetDetail=new SpreadSheetDetail();
					sheetDetail.setCreatedAt(rs.getString("DATECREATED"));
					sheetDetail.setUserId(rs.getString("USERID"));
					sheetDetail.setName(rs.getString("NAME"));
					resultList.add(sheetDetail);
				}
			}return new NounMetadata(resultList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String error = "Error in the reactor GetGoogleProfileReactor: " + e.getMessage();
			return new NounMetadata(error, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		}finally {
			if(rs!=null){
				try {
					rs.close();
				}catch(Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is user for getting list of all the details of users added to access google spreadsheet";
	}

}
