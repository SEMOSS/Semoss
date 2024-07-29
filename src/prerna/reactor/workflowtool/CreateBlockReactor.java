package prerna.reactor.workflowtool;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Utility;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.workflowtool.WorkflowBlockStatusUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class CreateBlockReactor extends AbstractReactor{
	public CreateBlockReactor() {
		// define the keys that will be used to fetch data for the database operation
		this.keysToGet = new String[] {"block_id", "notes"};
	}	

	@Override
	public NounMetadata execute() {
		// organize the keys to ensure they are ready for use
	    organizeKeys();
	    
	    // check to see if any of the important keys are given empty values
    	if (this.keyValue.get(this.keysToGet[0]).isEmpty()) {
    		return NounMetadata.getErrorNounMessage("block_id is required");
        }
	    
	    // setting the database engine
	    RDBMSNativeEngine database = (RDBMSNativeEngine)Utility.getDatabase("5cfd144f-b89e-47bb-9be9-5924e64dfc3a");
	    PreparedStatement insertQuery = null;
	    String workflow_notes;
	    String guid = UUID.randomUUID().toString();
	    String notes;
	    if(this.keyValue.get(this.keysToGet[1]) == null || this.keyValue.get(this.keysToGet[1]).trim().isEmpty()) {
	    	notes = "";
	    }else {
	    	notes = this.keyValue.get(this.keysToGet[1]);
	    }
	    try {
	    	System.out.println("connected to workflowtool Database");
	        java.sql.Date currDate;

	        // set the ps index to 1 because indexing for JDBC starts at 1
	        int i = 1;
	        insertQuery = database.getPreparedStatement(WorkflowBlockStatusUtil.createInsertPreparedStatementSql());
	        
	        // gets the current time and formats it
	        LocalDate localDate = LocalDate.now();
	        currDate = java.sql.Date.valueOf(localDate);
	        
	        // default notes 
			workflow_notes = "SYSTEM GENERATED - BLOCK GENERATED AND BEING MOVED TO SYSTEM";
			
			// we are going through the insertQuery prepared statement and replacing the ? with the correct corresponding values
	        insertQuery.setInt(i++, Integer.parseInt(this.keyValue.get(this.keysToGet[0])));
	        insertQuery.setString(i++, guid);
	        insertQuery.setInt(i++, 0);
	        insertQuery.setInt(i++,545);
	        insertQuery.setString(i++, "SYSTEM");
	        insertQuery.setString(i++, "NONE");
	        insertQuery.setString(i++, workflow_notes);
	        insertQuery.setDate(i++, currDate);
	        insertQuery.setString(i++, notes);
	        insertQuery.setBoolean(i++, false);
	        insertQuery.setBoolean(i++, true);

	        // executing the insert query we created with the values we want to add
	        insertQuery.execute();

	    } catch(SQLException e) {
	        e.printStackTrace();
	        return NounMetadata.getErrorNounMessage("SQL ERROR: " + e.getMessage());
	    }  finally {
	    	ConnectionUtils.closeAllConnectionsIfPooling(database,insertQuery);
	    }

	    return new NounMetadata("New Work Flow Block Created id: "+this.keyValue.get(this.keysToGet[0]), PixelDataType.CONST_STRING);
	}

	
}
