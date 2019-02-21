package prerna.engine.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;

public class InsightAdministrator {

	private static final Logger LOGGER = Logger.getLogger(InsightAdministrator.class.getName());
	private static final String TABLE_NAME = "QUESTION_ID";
	private static final String QUESTION_ID_COL = "ID";
	private static final String QUESTION_NAME_COL = "QUESTION_NAME";
	private static final String QUESTION_LAYOUT_COL = "QUESTION_LAYOUT";
	private static final String QUESTION_PKQL_COL = "QUESTION_PKQL";
	private static final String HIDDEN_INSIGHT_COL = "HIDDEN_INSIGHT";
	private static final String CACHEABLE_COL = "CACHEABLE";

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
		return addInsight(insightName, layout, pixelRecipeToSave, false);
	}
	
	public String addInsight(String insightName, String layout, String[] pixelRecipeToSave, boolean hidden) {
		String newId = UUID.randomUUID().toString();
		return addInsight(newId, insightName, layout, pixelRecipeToSave, hidden);
	}
	
	/**
	 * Insert a new insight into the engine
	 * @param insightId
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 */
	public String addInsight(String insightId, String insightName, String layout, String[] pixelRecipeToSave, boolean hidden) {
		LOGGER.info("Adding new question with insight id :::: " + insightId);
		LOGGER.info("Adding new question with name :::: " + insightName);
		LOGGER.info("Adding new question with layout :::: " + layout);
		LOGGER.info("Adding new question with recipe :::: " + Arrays.toString(pixelRecipeToSave));
		
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		layout = RdbmsQueryBuilder.escapeForSQLStatement(layout);
		
		StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(")
				.append(QUESTION_ID_COL).append(",").append(QUESTION_NAME_COL).append(",")
				.append(QUESTION_LAYOUT_COL).append(",").append(HIDDEN_INSIGHT_COL).append(",")
				.append(CACHEABLE_COL).append(",").append(QUESTION_PKQL_COL).append(") VALUES ('")
				.append(insightId).append("', ").append("'").append(insightName).append("', ")
				.append("'").append(layout).append("', ").append(hidden).append(", true, ");
		// loop through and add the recipe
		// don't forget to escape each entry in the array
		insertQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		insertQuery.append(");");
		
		// now run the query and commit
		try {
			this.insightEngine.insertData(insertQuery.toString());
			this.insightEngine.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// return the new rdbms id
		return insightId;
	}
	
	/**
	 * Update an existing insight
	 * @param existingRdbmsId
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 */
	public void updateInsight(String existingRdbmsId, String insightName, String layout, String[] pixelRecipeToSave) {
		updateInsight(existingRdbmsId, insightName, layout, pixelRecipeToSave, false);
	}
	
	public void updateInsight(String existingRdbmsId, String insightName, String layout, String[] pixelRecipeToSave, boolean hidden) {
		LOGGER.info("Modifying insight id :::: " + existingRdbmsId);
		LOGGER.info("Adding new question with name :::: " + insightName);
		LOGGER.info("Adding new question with layout :::: " + layout);
		LOGGER.info("Adding new question with recipe :::: " + Arrays.toString(pixelRecipeToSave));
		
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		layout = RdbmsQueryBuilder.escapeForSQLStatement(layout);
		
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(QUESTION_NAME_COL).append(" = '").append(insightName).append("', ")
				.append(QUESTION_LAYOUT_COL).append(" = '").append(layout).append("', ")
				.append(HIDDEN_INSIGHT_COL).append(" = '").append(hidden).append("', ")
				.append(QUESTION_PKQL_COL).append("=");
		updateQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		updateQuery.append(" WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
		
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
			this.insightEngine.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void updateInsightName(String existingRdbmsId, String insightName) {
		LOGGER.info("Modifying insight id :::: " + existingRdbmsId);
		LOGGER.info("Adding new question with name :::: " + insightName);
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(QUESTION_NAME_COL).append(" = '").append(insightName)
				.append("' WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
	
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.insightEngine.commit();
	}
	

	public void updateInsightCache(String existingRdbmsId, boolean parseBoolean) {
		LOGGER.info("Modifying insight id :::: " + existingRdbmsId);
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(CACHEABLE_COL).append(" = ").append(parseBoolean)
				.append(" WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
	
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		try {
			insightEngine.removeData(deleteQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Drop specific insights from the insight
	 * @param insightIDs
	 * @throws Exception 
	 */
	public void dropInsight(Collection<String> insightIDs) throws Exception {		
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
	
	private String createString(Collection<String> ids) {
		StringBuilder b = new StringBuilder("(");
		Iterator<String> iterator = ids.iterator();
		if(iterator.hasNext()) {
			b.append("'").append(iterator.next()).append("'");
		}
		while(iterator.hasNext()) {
			b.append(", '").append(iterator.next()).append("'");
		}
		b.append(")");
		return b.toString();
	}
	
	public static String getArraySqlSyntax(String[] pixelRecipeToSave) {
		StringBuilder sql = new StringBuilder("(");
		int numPixels = pixelRecipeToSave.length;
		for(int i = 0; i < numPixels; i++) {
			sql.append("'").append(RdbmsQueryBuilder.escapeForSQLStatement(pixelRecipeToSave[i])).append("'");
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
			sql.append("'").append(RdbmsQueryBuilder.escapeForSQLStatement(pixelRecipeToSave.get(i))).append("'");
			if(i+1 != numPixels) {
				sql.append(",");
			}
		}
		sql.append(")");
		return sql.toString();
	}
}
