package prerna.engine.impl;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;

public class InsightAdministrator {

	private static final Logger LOGGER = Logger.getLogger(InsightAdministrator.class.getName());
	private static final String TABLE_NAME = "QUESTION_ID";
	private static final String COL_QUESTION_ID = "ID";
	private static final String COL_QUESTION_NAME = "QUESTION_NAME";
	private static final String COL_QUESTION_LAYOUT = "QUESTION_LAYOUT";
	private static final String COL_QUESTION_PKQL = "QUESTION_PKQL";
//	private static final String GET_LAST_INSIGHT_ID = "SELECT DISTINCT " + COL_QUESTION_ID + " FROM " + TABLE_NAME + " ORDER BY " + COL_QUESTION_ID + " DESC";

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
		String newId = UUID.randomUUID().toString();
		
		StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(")
				.append(COL_QUESTION_ID).append(",").append(COL_QUESTION_NAME).append(",")
				.append(COL_QUESTION_LAYOUT).append(",").append(COL_QUESTION_PKQL).append(") VALUES ('")
				.append(newId).append("', ").append("'").append(insightName).append("', ")
				.append("'").append(layout).append("', ");
		// loop through and add the recipe
		// don't forget to escape each entry in the array
		insertQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		insertQuery.append(");");
		
		// now run the query and commit
		this.insightEngine.insertData(insertQuery.toString());
		this.insightEngine.commit();
		
		// return the new rdbms id
		return newId;
	}
	
	/**
	 * This method is used only when I know what insight id I want to insert into the database
	 * @param insightId
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 */
	public void addInsight(String insightId, String insightName, String layout, String[] pixelRecipeToSave) {
		LOGGER.info("Adding new question with insight id :::: " + insightId);
		LOGGER.info("Adding new question with name :::: " + insightName);
		LOGGER.info("Adding new question with layout :::: " + layout);
		LOGGER.info("Adding new question with recipe :::: " + Arrays.toString(pixelRecipeToSave));
		
		insightName = escapeForSQLStatement(insightName);
		layout = escapeForSQLStatement(layout);
		
		StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(")
				.append(COL_QUESTION_ID).append(",").append(COL_QUESTION_NAME).append(",")
				.append(COL_QUESTION_LAYOUT).append(",").append(COL_QUESTION_PKQL).append(") VALUES ('")
				.append(insightId).append("', ").append("'").append(insightName).append("', ")
				.append("'").append(layout).append("', ");
		// loop through and add the recipe
		// don't forget to escape each entry in the array
		insertQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		insertQuery.append(");");
		
		// now run the query and commit
		this.insightEngine.insertData(insertQuery.toString());
		this.insightEngine.commit();
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
				.append(COL_QUESTION_PKQL).append("=");
		updateQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		updateQuery.append(" WHERE ").append(COL_QUESTION_ID).append(" = '").append(existingRdbmsId).append("'");
		
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
	private static String escapeForSQLStatement(String s) {
		return s.replaceAll("'", "''");
	}
	
	public static String getArraySqlSyntax(String[] pixelRecipeToSave) {
		StringBuilder sql = new StringBuilder("(");
		int numPixels = pixelRecipeToSave.length;
		for(int i = 0; i < numPixels; i++) {
			sql.append("'").append(escapeForSQLStatement(pixelRecipeToSave[i])).append("'");
			if(i+1 != numPixels) {
				sql.append(",");
			}
		}
		sql.append(")");
		return sql.toString();
	}
	
	public static String getArraySqlSyntax(List<String> pixelRecipeToSave) {
		StringBuilder sql = new StringBuilder("(");
		int numPixels = pixelRecipeToSave.size();
		for(int i = 0; i < numPixels; i++) {
			sql.append("'").append(escapeForSQLStatement(pixelRecipeToSave.get(i))).append("'");
			if(i+1 != numPixels) {
				sql.append(",");
			}
		}
		sql.append(")");
		return sql.toString();
	}
}
