package prerna.reactor.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunSQLReactor extends AbstractReactor {

  public RunSQLReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.COMMAND.getKey()};
    this.keyRequired = new int[] {1, 1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    
    // check database permissions
    String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
    if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
		throw new IllegalArgumentException(
				"Database " + databaseId + " does not exist or user does not have access to this database");
	}
    
    IRDBMSEngine database = (RDBMSNativeEngine) Utility.getDatabase(databaseId);
    	
    String query = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
    
    Connection con = null;
	try {
		con = database.makeConnection();
	    try (PreparedStatement ps = con.prepareStatement(query)) {
	    	ResultSet rs = ps.executeQuery();
	    	
	    	// I can't find the Wrapper manager way of converting a result set to a map
	    	ResultSetMetaData rsmd = rs.getMetaData();
	    	List<Map<String, String>> columnInfo = new ArrayList<>();
	        int columnCount = rsmd.getColumnCount();
	        
	    	List<List<Object>> resultObject = new ArrayList<>();
	    	boolean gotMetadata = false;
	    	while (rs.next()) {
	    		List<Object> vals = new ArrayList<>();
	    		int columnIndex = 1;
	    		while (columnIndex < columnCount + 1) {
	    			if (!gotMetadata) {
	    				Map<String, String> col = new HashMap<>();
	    				col.put("key", rsmd.getColumnName(columnIndex));
	    				col.put("type", rsmd.getColumnTypeName(columnIndex));
	    				columnInfo.add(col);
	    			}
	    			
	    			vals.add(rs.getObject(columnIndex++));
	    		}
	    		gotMetadata = true;
	    		resultObject.add(vals);
	    	}
	    	Map<String, Object> finalResultMap = new HashMap<>();
	    	finalResultMap.put("columns", columnInfo);
	    	finalResultMap.put("rows", resultObject);
	    	return new NounMetadata(finalResultMap, PixelDataType.MAP);
	    } catch (SQLException e) {
	    	throw new SemossPixelException("Could not run generated SQL");
	    }
	} catch (Exception e) {
		throw new IllegalArgumentException("Error occured establishing connection to database: " + e.getMessage());
	} finally {
		try {
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}    
  }
}
