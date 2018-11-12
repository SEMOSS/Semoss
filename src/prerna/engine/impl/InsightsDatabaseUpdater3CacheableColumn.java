package prerna.engine.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;

@Deprecated
public class InsightsDatabaseUpdater3CacheableColumn {

	/*
	 * THIS CLASS IS HERE DUE TO A NEW COLUMN THAT IS REQUIRED TO TOGGLE CACHING ON/OFF FOR INSIGHTS
	 * 
	 * 		CACHEABLE BOOLEAN
	 * 
	 * 
	 * THIS WILL ALSO REMOVE SOME COLUMNS WE DO NOT NEED
	 * 
	 * 		QUESTION_PROPERTIES CLOB
	 *		QUESTION_OWL CLOB
	 * 		QUESTION_IS_DB_QUERY BOOLEAN
	 * 
	 */
	
	private static final String UPDATE_QUERY = "ALTER TABLE QUESTION_ID ADD COLUMN IF NOT EXISTS CACHEABLE BOOLEAN DEFAULT TRUE";
	private static final String[] COLUMNS_TO_DELETE = {"QUESTION_PROPERTIES", "QUESTION_OWL", "QUESTION_IS_DB_QUERY"};

	
	@Deprecated
	public static void update(String appId, RDBMSNativeEngine insightsRdbms) {
		
		boolean tableExists = false;
		ResultSet rs = null;
		try {
			rs = insightsRdbms.getConnectionMetadata().getTables(null, null, "QUESTION_ID", null);
			if (rs.next()) {
				  tableExists = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
		
		if(!tableExists) {
			// well, you already created the file
			// need to run the queries to make this
			UploadUtilities.runInsightCreateTableQueries(insightsRdbms);
			UploadUtilities.addExploreInstanceInsight(appId, insightsRdbms);
		} else {
			// first let us drop the tables we do not require
			{
				for(String col : COLUMNS_TO_DELETE) {
					String dropColQuery = "ALTER TABLE QUESTION_ID DROP COLUMN IF EXISTS " + col;
					try {
						insightsRdbms.removeData(dropColQuery);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
			
			// let us add the new column with default value of true
			{
				try {
					insightsRdbms.insertData(UPDATE_QUERY);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			insightsRdbms.commit();
		}
	}
	
}
