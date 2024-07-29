/**
 * 
 */
package prerna.reactor.workflowtool;

import java.util.UUID;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.time.*;
import java.io.IOException;

import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.util.Utility;
import prerna.reactor.workflowtool.WorkflowBlockStatusUtil;

public class MoveBlockReactor extends AbstractReactor {
 public MoveBlockReactor() {
     // Define the keys that will be used to fetch data for the database operation
     this.keysToGet = new String[] {"block_id", "guid", "previous_stage", "current_stage"};
 }
 @Override
 public NounMetadata execute() {
     // Organize the keys to ensure they are ready for use
     organizeKeys();
     Boolean hasGuid;
     int block_id;
     
     // Enforcing that the user send's a block_id
     if(this.keyValue.get(this.keysToGet[0]).trim().isEmpty()) {
    	 return NounMetadata.getErrorNounMessage("block_id is required");
     }else {
    	 block_id = Integer.parseInt(this.keyValue.get(this.keysToGet[0]));
     }
     
     // Retrieving key values so we are able to use them
     String guid = this.keyValue.get(this.keysToGet[1]);
     String previous_stage = this.keyValue.get(this.keysToGet[2]);
     String current_stage = this.keyValue.get(this.keysToGet[3]);
     if(guid != null && !guid.trim().isEmpty()) {
         hasGuid = true;
     } else {
         hasGuid = false;
     }

     // Check if any required keys are missing or empty
     if(previous_stage.trim().isEmpty() || current_stage.trim().isEmpty()) {
    	 return NounMetadata.getErrorNounMessage("previous_stage and current_stage are required");
     }
 
     // Connect to the database
     RDBMSNativeEngine database = (RDBMSNativeEngine)Utility.getDatabase("5cfd144f-b89e-47bb-9be9-5924e64dfc3a");

     // Prepare the date formatter and parse the date
     LocalDate localDate = LocalDate.now();
     java.sql.Date currDate = java.sql.Date.valueOf(localDate);

     // Setting up indexes so we can use them below when forming our insert Query
     int block_id_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("block_id");
     int guid_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("guid");
     int sender_id_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("sender_id");
     int receiver_id_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("receiver_id");
     int current_stage_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("current_stage");
     int previous_stage_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("previous_stage");
     int workflow_notes_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("workflow_notes");
     int mod_date_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("mod_date");
     int notes_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("notes");
     int is_assigned_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("is_assigned");
     int is_latest_index = WorkflowBlockStatusUtil.BLOCK_COLUMN_NAME_LIST.indexOf("is_latest");

     // Creating a query struct to fetch the data from the database
     SelectQueryStruct qs = WorkflowBlockStatusUtil.getAllWorkflowSelectorQs();
     // Filtering through so you are able to get the row you want
     if (hasGuid && hasGuid != null) {
         qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(WorkflowBlockStatusUtil.BLOCK_PREFIX+"is_latest", "==", true));
         qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(WorkflowBlockStatusUtil.BLOCK_PREFIX+"block_id", "==", block_id));
         qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(WorkflowBlockStatusUtil.BLOCK_PREFIX+"guid", "==", guid));
     }else {
         qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(WorkflowBlockStatusUtil.BLOCK_PREFIX+"is_latest", "==", true));
         qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(WorkflowBlockStatusUtil.BLOCK_PREFIX+"block_id", "==", block_id));
     }

     // Log the query structure for debugging
     System.out.println("Query Structure: " + qs.toString());

     // synchronized the block to ensure thread safety
     synchronized(MoveBlockReactor.class) {
         PreparedStatement updateQuery = null;
         PreparedStatement insertQuery = null;
         IRawSelectWrapper iterator = null;
         String newGuid = UUID.randomUUID().toString();

         try {
             // Gets the data with a database query and you get values one by one
             iterator = WrapperManager.getInstance().getRawWrapper(database, qs);
             if(!iterator.hasNext()) {
            	 // Sending an error string to let the user know that the block_id doesn't exist
                 return NounMetadata.getErrorNounMessage("Cannot find block ("+block_id+") or block has moved to a new stage");
             }
             if(iterator.hasNext()) {
                 if (!hasGuid) {
                     updateQuery = database.getPreparedStatement(WorkflowBlockStatusUtil.createUpdatePreparedStatementSqlNoGuid());
                 } else {
                     updateQuery = database.getPreparedStatement(WorkflowBlockStatusUtil.createUpdatePreparedStatementSql());
                 }
                 insertQuery = database.getPreparedStatement(WorkflowBlockStatusUtil.createInsertPreparedStatementSql());
             }

             // Setting Up the Update Query String
             if(hasGuid) {
                 int updateIndex = 1;
                 updateQuery.setInt(updateIndex++, block_id);
                 updateQuery.setString(updateIndex++, guid);
             } else {
                 int updateIndex = 1;
                 updateQuery.setInt(updateIndex, block_id);
             }

             /// Fetch the data from the iterator and we can run through it one by one
             Object[] data = iterator.next().getValues();
             int insertIndex = 1;
             // Populating the insert query with the correct data
             for(int i = 0; i < data.length; i++) {
                 Object val = data[i];
                 if(val == null) {
                     // Handle null values appropriately
                     System.out.println("Null value found at index: " + i);
                     if(i == 2 || i == 5) {
                    	 val = "NONE";
                     }
                     if(i == 3) {
                    	 val = "SYSTEM";
                     }
                 }
                 if(i == block_id_index)
                     insertQuery.setInt(insertIndex++, (Integer)val);
                 else if(i == guid_index)
                     insertQuery.setString(insertIndex++, newGuid);
                 else if(i == sender_id_index)
                     insertQuery.setInt(insertIndex++, (Integer)val);
                 else if(i == receiver_id_index)
                     insertQuery.setInt(insertIndex++, (Integer)val);
                 else if(i == current_stage_index )
                     insertQuery.setString(insertIndex++, current_stage);
                 else if(i == previous_stage_index)
                     insertQuery.setString(insertIndex++, previous_stage);
                 else if(i == workflow_notes_index) {
                     String notes = val.toString() + "\nSYSTEM GENERATED: BLOCK IS MOVING FROM "+previous_stage+" to "+current_stage + " on "+currDate.toString();
                     insertQuery.setString(insertIndex++, notes);
                 }
                 else if(i == mod_date_index)
                     insertQuery.setDate(insertIndex++, currDate);
                 else if(i == notes_index)
                     insertQuery.setString(insertIndex++, val.toString());
                 else if(i == is_assigned_index)
                     insertQuery.setBoolean(insertIndex++, (Boolean)val);
                 else if(i == is_latest_index)
                     insertQuery.setBoolean(insertIndex++, true);
             }
             try {
            	 //Executing the update and insert queries 
                 updateQuery.execute();
                 insertQuery.execute(); 
             }catch(SQLException e) {
                 return NounMetadata.getErrorNounMessage("Error executing SQL. Connection rolled back. Error: "+e.getMessage());
             }

         }catch(Exception e) {
             return NounMetadata.getErrorNounMessage("Error in moving the Block: "+e.getMessage());
         }finally {
             // Closing all the Connections and making sure there are NO errors doing so
             if(iterator != null) {
                 try {
                     iterator.close();
                 }catch(IOException e) {
                     return NounMetadata.getErrorNounMessage("Error closing iterator. Error: "+e.getMessage());
                 }
                 try {
                     insertQuery.close();
                     updateQuery.close();
                 }catch(SQLException e) {
                     return NounMetadata.getErrorNounMessage("Error closing iterator. Error: "+e.getMessage());
                 }
             }
         }
     }
     // returning a string to the front end so they know the operation was completed successfully
     return new NounMetadata("Successfully Moved Block:("+block_id+")"+" from stage: ("+previous_stage.toUpperCase()+")"+" to stage: ("+current_stage.toUpperCase()+")", PixelDataType.CONST_STRING);
 }
}

