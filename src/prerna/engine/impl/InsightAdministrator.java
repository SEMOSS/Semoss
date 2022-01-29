package prerna.engine.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class InsightAdministrator {

	private static final Logger logger = LogManager.getLogger(InsightAdministrator.class);
	
	private static final String TABLE_NAME = "QUESTION_ID";
	private static final String QUESTION_ID_COL = "ID";
	private static final String QUESTION_NAME_COL = "QUESTION_NAME";
	private static final String QUESTION_LAYOUT_COL = "QUESTION_LAYOUT";
	private static final String QUESTION_PKQL_COL = "QUESTION_PKQL";
	private static final String HIDDEN_INSIGHT_COL = "HIDDEN_INSIGHT";
	private static final String CACHEABLE_COL = "CACHEABLE";
	private static final String CACHE_MINUTES_COL = "CACHE_MINUTES";

	private static Gson gson = new Gson();

	private RDBMSNativeEngine insightEngine;
	private AbstractSqlQueryUtil queryUtil;
	private boolean allowArrayDatatype;
	
	public InsightAdministrator(RDBMSNativeEngine insightEngine) {
		this.insightEngine = insightEngine;
		this.queryUtil = this.insightEngine.getQueryUtil();
		this.allowArrayDatatype = this.queryUtil.allowArrayDatatype();
	}

	/**
	 * 
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 * @param hidden
	 * @param cacheable
	 * @param cacheMinutes
	 * @return
	 */
	public String addInsight(String insightName, String layout, String[] pixelRecipeToSave, boolean hidden, 
			boolean cacheable, int cacheMinutes) {
		String newId = UUID.randomUUID().toString();
		return addInsight(newId, insightName, layout, pixelRecipeToSave, hidden, cacheable, cacheMinutes);
	}
	
	/**
	 * Insert a new insight into the engine
	 * @param insightId
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 * @param hidden
	 * @param cacheable
	 * @param minutesForCache
	 * @return
	 */
	public String addInsight(String insightId, String insightName, String layout, String[] pixelRecipeToSave, 
			boolean hidden, boolean cacheable, int minutesForCache) {
		logger.info("Adding new question with insight id :::: " + Utility.cleanLogString(insightId));
		logger.info("Adding new question with name :::: " + Utility.cleanLogString(insightName));
		logger.info("Adding new question with layout :::: " + Utility.cleanLogString(layout));
		logger.info("Adding new question with recipe :::: " + Utility.cleanLogString(Arrays.toString(pixelRecipeToSave)));
		
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		layout = RdbmsQueryBuilder.escapeForSQLStatement(layout);
		
		StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(")
				.append(QUESTION_ID_COL).append(",").append(QUESTION_NAME_COL).append(",")
				.append(QUESTION_LAYOUT_COL).append(",").append(HIDDEN_INSIGHT_COL).append(",")
				.append(CACHEABLE_COL).append(",").append(QUESTION_PKQL_COL).append(") VALUES ('")
				.append(insightId).append("', ").append("'").append(insightName).append("', ")
				.append("'").append(layout).append("', ").append(hidden).append(", ").append(cacheable).append(", ");
		// loop through and add the recipe
		// don't forget to escape each entry in the array
		if(this.allowArrayDatatype) {
			insertQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		} else {
			insertQuery.append(getClobRecipeSyntax(pixelRecipeToSave));
		}
		insertQuery.append(");");
		
		// now run the query and commit
		try {
			this.insightEngine.insertData(insertQuery.toString());
			this.insightEngine.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		// return the new rdbms id
		return insightId;
	}

	public String addInsight(String insightName, String layout, Collection<String> pixelRecipeToSave, boolean hidden, 
			boolean cacheable, int cacheMinutes) {
		String newId = UUID.randomUUID().toString();
		return addInsight(newId, insightName, layout, pixelRecipeToSave, hidden, cacheable, cacheMinutes);
	}
	
	/**
	 * Insert a new insight into the engine
	 * @param insightId
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 */
	public String addInsight(final String insightId, String insightName, String layout, Collection<String> pixelRecipeToSave,
		boolean hidden, boolean cacheable, int minutesForCache) {
		logger.info("Adding new question with insight id :::: " + insightId);
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		layout = RdbmsQueryBuilder.escapeForSQLStatement(layout);
		
		StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(TABLE_NAME).append("(")
				.append(QUESTION_ID_COL).append(",").append(QUESTION_NAME_COL).append(",")
				.append(QUESTION_LAYOUT_COL).append(",").append(HIDDEN_INSIGHT_COL).append(",")
				.append(CACHEABLE_COL).append(",").append(QUESTION_PKQL_COL).append(") VALUES ('")
				.append(insightId).append("', ").append("'").append(insightName).append("', ")
				.append("'").append(layout).append("', ").append(hidden).append(", ").append(cacheable).append(", ");
		// loop through and add the recipe
		// don't forget to escape each entry in the array
		if(this.allowArrayDatatype) {
			insertQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		} else {
			insertQuery.append(getClobRecipeSyntax(pixelRecipeToSave));
		}
		insertQuery.append(");");
		
		// now run the query and commit
		try {
			this.insightEngine.insertData(insertQuery.toString());
			this.insightEngine.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		// return the new rdbms id
		return insightId;
	}
	
	/**
	 * Update the insight tags for the insight
	 * Will delete existing values and then perform a bulk insert
	 * @param insightId
	 * @param tags
	 */
	public void updateInsightTags(String insightId, List<String> tags) {
		// first do a delete
		String query = "DELETE FROM INSIGHTMETA WHERE METAKEY='tag' AND INSIGHTID='" + insightId + "'";
		try {
			this.insightEngine.insertData(query);
			this.insightEngine.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		if(tags != null && !tags.isEmpty()) {
			// now we do the new insert with the order of the tags
			query = this.queryUtil.createInsertPreparedStatementString("INSIGHTMETA", 
					new String[]{"INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"});
			PreparedStatement ps = null;
			try {
				ps = this.insightEngine.getPreparedStatement(query);
				for(int i = 0; i < tags.size(); i++) {
					String tag = tags.get(i);
					ps.setString(1, insightId);
					ps.setString(2, "tag");
					ps.setString(3, tag);
					ps.setInt(4, i);
					ps.addBatch();;
				}
				
				ps.executeBatch();
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update the insight tags for the insight
	 * Will delete existing values and then perform a bulk insert
	 * @param insightId
	 * @param tags
	 */
	public void updateInsightTags(String insightId, String[] tags) {
		// first do a delete
		String query = "DELETE FROM INSIGHTMETA WHERE METAKEY='tag' AND INSIGHTID='" + insightId + "'";
		try {
			this.insightEngine.insertData(query);
			this.insightEngine.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		if(tags != null && tags.length > 0) {
			// now we do the new insert with the order of the tags
			query = this.queryUtil.createInsertPreparedStatementString("INSIGHTMETA", 
					new String[]{"INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"});
			PreparedStatement ps = null;
			try {
				ps = this.insightEngine.getPreparedStatement(query);
				for(int i = 0; i < tags.length; i++) {
					String tag = tags[i];
					ps.setString(1, insightId);
					ps.setString(2, "tag");
					ps.setString(3, tag);
					ps.setInt(4, i);
					ps.addBatch();;
				}
				
				ps.executeBatch();
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update the insight description
	 * Will perform an insert if the description doesn't currently exist
	 * @param insideId
	 * @param description
	 */
	public void updateInsightDescription(String insightId, String description) {
		// try to do an update
		// if nothing is updated
		// do an insert
		insightId = RdbmsQueryBuilder.escapeForSQLStatement(insightId);
		String query = "UPDATE INSIGHTMETA SET METAVALUE='" 
				+ AbstractSqlQueryUtil.escapeForSQLStatement(description) + "' "
				+ "WHERE METAKEY='description' AND INSIGHTID='" + insightId + "'";
		Statement stmt = null;
		try {
			stmt = this.insightEngine.execUpdateAndRetrieveStatement(query, false);
			if(stmt.getUpdateCount() == 0) {
				// need to perform an insert
				query = this.queryUtil.insertIntoTable("INSIGHTMETA", 
						new String[]{"INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"}, 
						new String[]{"varchar(255)", "varchar(255)", "clob", "int"}, 
						new Object[]{insightId, "description", description, 0});
				this.insightEngine.insertData(query);
			}
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	
	public void updateInsight(String existingRdbmsId, String insightName, String layout, String[] pixelRecipeToSave, 
			boolean hidden, boolean cacheable, int cacheMinutes) {
		logger.info("Modifying insight id :::: " + Utility.cleanLogString(existingRdbmsId));
		logger.info("Adding new question with name :::: " + Utility.cleanLogString(insightName));
		logger.info("Adding new question with layout :::: " + Utility.cleanLogString(layout));
		logger.info("Adding new question with recipe :::: " + Utility.cleanLogString(Arrays.toString(pixelRecipeToSave)));
		
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		layout = RdbmsQueryBuilder.escapeForSQLStatement(layout);
		
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(QUESTION_NAME_COL).append(" = '").append(insightName).append("', ")
				.append(QUESTION_LAYOUT_COL).append(" = '").append(layout).append("', ")
				.append(HIDDEN_INSIGHT_COL).append(" = ").append(hidden).append(", ")
				.append(QUESTION_PKQL_COL).append("=");
		if(this.allowArrayDatatype) {
			updateQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		} else {
			updateQuery.append(getClobRecipeSyntax(pixelRecipeToSave));
		}
		updateQuery.append(" WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
		
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
			this.insightEngine.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void updateInsight(String existingRdbmsId, String insightName, String layout, Collection<String> pixelRecipeToSave, 
			boolean hidden, boolean cacheable, int cacheMinutes) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);
		
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		layout = RdbmsQueryBuilder.escapeForSQLStatement(layout);
		
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(QUESTION_NAME_COL).append(" = '").append(insightName).append("', ")
				.append(QUESTION_LAYOUT_COL).append(" = '").append(layout).append("', ")
				.append(HIDDEN_INSIGHT_COL).append(" = ").append(hidden).append(", ")
				.append(QUESTION_PKQL_COL).append("=");
		if(this.allowArrayDatatype) {
			updateQuery.append(getArraySqlSyntax(pixelRecipeToSave));
		} else {
			updateQuery.append(getClobRecipeSyntax(pixelRecipeToSave));
		}
		updateQuery.append(" WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
		
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
			this.insightEngine.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	public void updateInsightName(String existingRdbmsId, String insightName) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);
		logger.info("Adding new question with name :::: " + insightName);
		insightName = RdbmsQueryBuilder.escapeForSQLStatement(insightName);
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(QUESTION_NAME_COL).append(" = '").append(insightName)
				.append("' WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
	
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		this.insightEngine.commit();
	}
	

	public void updateInsightCache(String existingRdbmsId, boolean isCacheable, int cacheMinutes) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(CACHEABLE_COL).append(" = ").append(isCacheable)
				.append(" WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
	
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		this.insightEngine.commit();
	}
	
	public void updateInsightGlobal(String existingRdbmsId, boolean isHidden) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);
		StringBuilder updateQuery = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ")
				.append(HIDDEN_INSIGHT_COL).append(" = ").append(isHidden)
				.append(" WHERE ").append(QUESTION_ID_COL).append(" = '").append(existingRdbmsId).append("'");
	
		// now run the query and commit
		try {
			this.insightEngine.insertData(updateQuery.toString());
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
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
		logger.info("Running drop query :::: " + Utility.cleanLogString(deleteQuery));
		try {
			insightEngine.removeData(deleteQuery);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		deleteQuery = "DELETE FROM INSIGHTMETA WHERE INSIGHTID IN " + idsString;
		logger.info("Running drop query :::: " + Utility.cleanLogString(deleteQuery));
		try {
			insightEngine.removeData(deleteQuery);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
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
		logger.info("Running drop query :::: " + Utility.cleanLogString(deleteQuery));
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
			idsString = idsString + "'" + RdbmsQueryBuilder.escapeForSQLStatement(id) + "', ";
		}
		idsString = idsString.substring(0, idsString.length() - 2) + ")";
		
		return idsString;
	}
	
	private String createString(Collection<String> ids) {
		StringBuilder b = new StringBuilder("(");
		Iterator<String> iterator = ids.iterator();
		if(iterator.hasNext()) {
			b.append("'").append(RdbmsQueryBuilder.escapeForSQLStatement(iterator.next())).append("'");
		}
		while(iterator.hasNext()) {
			b.append(", '").append(RdbmsQueryBuilder.escapeForSQLStatement(iterator.next())).append("'");
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
	
	public static String getArraySqlSyntax(Collection<String> pixelRecipeToSave) {
		StringBuilder sql = new StringBuilder("(");
		Iterator<String> it = pixelRecipeToSave.iterator();
		if(it.hasNext()) {
			sql.append("'").append(RdbmsQueryBuilder.escapeForSQLStatement(it.next())).append("'");
		}
		while(it.hasNext()) {
			sql.append(",'").append(RdbmsQueryBuilder.escapeForSQLStatement(it.next())).append("'");
		}
		sql.append(")");
		return sql.toString();
	}
	
	public static String getClobRecipeSyntax(String[] pixelRecipeToSave) {
		String sql = gson.toJson(pixelRecipeToSave);
		return "'" + RdbmsQueryBuilder.escapeForSQLStatement(sql) + "'";
	}
	
	public static String getClobRecipeSyntax(Collection<String> pixelRecipeToSave) {
		return getClobRecipeSyntax(pixelRecipeToSave.toArray(new String[pixelRecipeToSave.size()]));
	}
}
