package prerna.engine.impl;

import java.util.Arrays;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class InsightAdministrator {

	private static final Logger LOGGER = Logger.getLogger(InsightAdministrator.class.getName());
	private static final String TABLE_NAME = "QUESTION_ID";
	private static final String COL_QUESTION_ID = "ID";
	private static final String COL_QUESTION_NAME = "QUESTION_NAME";
	private static final String COL_QUESTION_LAYOUT = "QUESTION_LAYOUT";
	private static final String COL_QUESTION_PKQL = "QUESTION_PKQL";
	private static final String GET_LAST_INSIGHT_ID = "SELECT DISTINCT " + COL_QUESTION_ID + " FROM " + TABLE_NAME + " ORDER BY " + COL_QUESTION_ID + " DESC";

	private IEngine insightEngine;
	
	public InsightAdministrator(IEngine insightEngine) {
		this.insightEngine = insightEngine;
	}

	/**
	 * Insert a new insight into the engine
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 * @return
	 */
	public String addInsight(String insightName, String layout, String[] pixelRecipeToSave) {
		LOGGER.info("Adding new question with name :::: " + insightName);
		LOGGER.info("Adding new question with layout :::: " + layout);
		LOGGER.info("Adding new question with recipe :::: " + Arrays.toString(pixelRecipeToSave));
		
		insightName = escapeForSQLStatement(insightName);
		layout = escapeForSQLStatement(layout);
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(insightEngine, GET_LAST_INSIGHT_ID);
		Object lastIdNum = 0;
		if(wrapper.hasNext()){ // need to call hasNext before you call next()
			lastIdNum = wrapper.next().getValues()[0];
		}
		String lastIDNum = ((int)lastIdNum+1) + "";
		
		StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(")
				.append(COL_QUESTION_ID).append(",").append(COL_QUESTION_NAME).append(",")
				.append(COL_QUESTION_LAYOUT).append(",").append(COL_QUESTION_PKQL).append(") VALUES (")
				.append(lastIDNum).append(", ").append("'").append(insightName).append("', ")
				.append("'").append(layout).append("', (");
		// loop through and add the recipe
		// don't forget to escape each entry in the array
		int numPixels = pixelRecipeToSave.length;
		for(int i = 0; i < numPixels; i++) {
			insertQuery.append("'").append(escapeForSQLStatement(pixelRecipeToSave[i])).append("'");
			if(i+1 != numPixels) {
				insertQuery.append(",");
			}
		}
		insertQuery.append("));");
		
		// now run the query and commit
		this.insightEngine.insertData(insertQuery.toString());
		this.insightEngine.commit();
		
		// return the new rdbms id
		return lastIDNum;
	}
	
	/**
	 * Update an existing insight
	 * @param existingRdbmsId
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 */
	public void updateInsight(String existingRdbmsId, String insightName, String layout, String[] pixelRecipeToSave) {
		LOGGER.info("Modifying insert id :::: " + existingRdbmsId);
		LOGGER.info("Adding new question with name :::: " + insightName);
		LOGGER.info("Adding new question with layout :::: " + layout);
		LOGGER.info("Adding new question with recipe :::: " + Arrays.toString(pixelRecipeToSave));
		
		insightName = escapeForSQLStatement(insightName);
		layout = escapeForSQLStatement(layout);
		
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(COL_QUESTION_NAME).append(" = '").append(insightName).append("', ")
				.append(COL_QUESTION_LAYOUT).append(" = '").append(layout).append("', ")
				.append(COL_QUESTION_PKQL).append("=(");
		// loop through and add the recipe
		// don't forget to escape each entry in the array
		int numPixels = pixelRecipeToSave.length;
		for(int i = 0; i < numPixels; i++) {
			updateQuery.append("'").append(escapeForSQLStatement(pixelRecipeToSave[i])).append("'");
			if(i+1 != numPixels) {
				updateQuery.append(",");
			}
		}
		updateQuery.append(") WHERE ").append(COL_QUESTION_ID).append(" = ").append(existingRdbmsId);
		
		// now run the query and commit
		this.insightEngine.insertData(updateQuery.toString());
		this.insightEngine.commit();
	}
	
	/**
	 * Drop specific insights from the insight
	 * @param insightIDs
	 */
	public void dropInsight(String... insightIDs) {		
		String idsString = createString(insightIDs);
		String deleteQuery = "DELETE FROM QUESTION_ID WHERE ID IN " + idsString;
		LOGGER.info("Running drop query :::: " + deleteQuery);
		insightEngine.removeData(deleteQuery);
	}
	
	/**
	 * Genereate the sql portion that uses a set of insight ids
	 * @param ids
	 * @return
	 */
	private String createString(String... ids){
		String idsString = "(";
		for(String id : ids){
			idsString = idsString + "'" + id + "', ";
		}
		idsString = idsString.substring(0, idsString.length() - 2) + ")";
		
		return idsString;
	}
	
	/**
	 * Need to escape single quotes for sql queries
	 * @param s
	 * @return
	 */
	private String escapeForSQLStatement(String s) {
		return s.replaceAll("'", "''");
	}
}
