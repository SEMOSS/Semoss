package prerna.engine.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
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

	public static final String TABLE_NAME = "QUESTION_ID";
	public static final String QUESTION_ID_COL = "ID";
	public static final String QUESTION_NAME_COL = "QUESTION_NAME";
	public static final String QUESTION_LAYOUT_COL = "QUESTION_LAYOUT";
	public static final String QUESTION_PKQL_COL = "QUESTION_PKQL";
	public static final String HIDDEN_INSIGHT_COL = "HIDDEN_INSIGHT";
	public static final String CACHEABLE_COL = "CACHEABLE";
	public static final String CACHE_MINUTES_COL = "CACHE_MINUTES";
	public static final String CACHE_CRON_COL = "CACHE_CRON";
	public static final String CACHED_ON_COL = "CACHED_ON";
	public static final String CACHE_ENCRYPT_COL = "CACHE_ENCRYPT";

	private static Gson gson = new Gson();

	private RDBMSNativeEngine insightEngine;
	private AbstractSqlQueryUtil queryUtil;
	private boolean allowArrayDatatype;
	private boolean allowClobJavaObject;

	public InsightAdministrator(RDBMSNativeEngine insightEngine) {
		this.insightEngine = insightEngine;
		this.queryUtil = this.insightEngine.getQueryUtil();
		this.allowArrayDatatype = this.queryUtil.allowArrayDatatype();
		this.allowClobJavaObject = this.queryUtil.allowClobJavaObject();
	}

	//TODO: CONVERT TO PREPARED STATEMENTS!!!

	public String addInsight(String insightName, String layout, Collection<String> pixelRecipeToSave, boolean hidden, 
			boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		return addInsight(insightName, layout, pixelRecipeToSave.toArray(new String[] {}), hidden, 
				cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt);
	}

	public String addInsight(final String insightId, String insightName, String layout, Collection<String> pixelRecipeToSave,
			boolean hidden, boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		return addInsight(insightId, insightName, layout, pixelRecipeToSave.toArray(new String[] {}), hidden, 
				cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt);
	}

	/**
	 * 
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 * @param hidden
	 * @param cacheable
	 * @param cacheMinutes
	 * @param cacheEncrypt
	 * @return
	 */
	public String addInsight(String insightName, String layout, String[] pixelRecipeToSave, boolean hidden, 
			boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		String newId = UUID.randomUUID().toString();
		return addInsight(newId, insightName, layout, pixelRecipeToSave, hidden, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt);
	}

	/**
	 * 
	 * @param insightId
	 * @param insightName
	 * @param layout
	 * @param pixelRecipeToSave
	 * @param hidden
	 * @param cacheable
	 * @param cacheMinutes
	 * @param cacheEncrypt
	 * @return
	 */
	public String addInsight(String insightId, String insightName, String layout, String[] pixelRecipeToSave, 
			boolean hidden, boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		logger.info("Adding new question with insight id :::: " + Utility.cleanLogString(insightId));
		logger.info("Adding new question with name :::: " + Utility.cleanLogString(insightName));
		logger.info("Adding new question with layout :::: " + Utility.cleanLogString(layout));
		logger.info("Adding new question with recipe :::: " + Utility.cleanLogString(Arrays.toString(pixelRecipeToSave)));

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));

		PreparedStatement ps = null;
		try {
			ps = insightEngine.bulkInsertPreparedStatement(new String[] {
					TABLE_NAME, QUESTION_ID_COL, QUESTION_NAME_COL, QUESTION_LAYOUT_COL, 
					HIDDEN_INSIGHT_COL, CACHEABLE_COL, CACHE_MINUTES_COL, 
					CACHE_CRON_COL, CACHED_ON_COL, CACHE_ENCRYPT_COL, QUESTION_PKQL_COL
			});

			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, insightName);
			ps.setString(parameterIndex++, layout);
			ps.setBoolean(parameterIndex++, hidden);
			ps.setBoolean(parameterIndex++, cacheable);
			ps.setInt(parameterIndex++, cacheMinutes);
			if(cacheCron == null) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, cacheCron);
			}
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setBoolean(parameterIndex++, cacheEncrypt);
			if(this.allowArrayDatatype) {
				java.sql.Array array = ps.getConnection().createArrayOf("VARCHAR", pixelRecipeToSave);
				ps.setArray(parameterIndex++, array);
			} else if(this.allowClobJavaObject) {
				java.sql.Clob clob = ps.getConnection().createClob();
				clob.setString(1, getClobRecipeSyntax(pixelRecipeToSave));
				ps.setClob(parameterIndex++, clob);
			} else {
				ps.setString(parameterIndex++, getClobRecipeSyntax(pixelRecipeToSave));
			}
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(insightEngine.isConnectionPooling()) {
				try {
					if(ps!=null) {
						ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
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
			boolean hidden, boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		logger.info("Modifying insight id :::: " + Utility.cleanLogString(existingRdbmsId));
		logger.info("Adding new question with name :::: " + Utility.cleanLogString(insightName));
		logger.info("Adding new question with layout :::: " + Utility.cleanLogString(layout));
		logger.info("Adding new question with recipe :::: " + Utility.cleanLogString(Arrays.toString(pixelRecipeToSave)));

		String query = "UPDATE " + TABLE_NAME + " SET "
				+ QUESTION_NAME_COL+"=?, "
				+ QUESTION_LAYOUT_COL+"=?, "
				+ HIDDEN_INSIGHT_COL+"=?, "
				+ CACHEABLE_COL+"=?, "
				+ CACHE_MINUTES_COL+"=?, "
				+ CACHE_CRON_COL+"=?, "
				+ CACHED_ON_COL+"=?, "
				+ CACHE_ENCRYPT_COL+"=?, "
				+ QUESTION_PKQL_COL+"=? WHERE "
				+ QUESTION_ID_COL+"=?";

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));

		PreparedStatement ps = null;
		try {
			ps = insightEngine.getPreparedStatement(query);

			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightName);
			ps.setString(parameterIndex++, layout);
			ps.setBoolean(parameterIndex++, hidden);
			ps.setBoolean(parameterIndex++, cacheable);
			ps.setInt(parameterIndex++, cacheMinutes);
			if(cacheCron == null || cacheCron.isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, cacheCron);
			}
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setBoolean(parameterIndex++, cacheEncrypt);
			if(this.allowArrayDatatype) {
				java.sql.Array array = ps.getConnection().createArrayOf("VARCHAR", pixelRecipeToSave);
				ps.setArray(parameterIndex++, array);
			} else if(this.allowClobJavaObject) {
				java.sql.Clob clob = ps.getConnection().createClob();
				clob.setString(1, getClobRecipeSyntax(pixelRecipeToSave));
				ps.setClob(parameterIndex++, clob);
			} else {
				ps.setString(parameterIndex++, getClobRecipeSyntax(pixelRecipeToSave));
			}
			ps.setString(parameterIndex++, existingRdbmsId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(insightEngine.isConnectionPooling()) {
				try {
					if(ps!=null) {
						ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	public void updateInsight(String existingRdbmsId, String insightName, String layout, Collection<String> pixelRecipeToSave, 
			boolean hidden, boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		updateInsight(existingRdbmsId, insightName, layout, pixelRecipeToSave.toArray(new String[] {}), 
				hidden, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt);
	}

	public void updateInsightName(String existingRdbmsId, String insightName) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);
		logger.info("Updating question name to :::: " + insightName);

		String query = "UPDATE " + TABLE_NAME + " SET "
				+ QUESTION_NAME_COL+"=? WHERE "
				+ QUESTION_ID_COL+"=?";

		PreparedStatement ps = null;
		try {
			ps = insightEngine.getPreparedStatement(query);

			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightName);
			ps.setString(parameterIndex++, existingRdbmsId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(insightEngine.isConnectionPooling()) {
				try {
					if(ps!=null) {
						ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}


	public void updateInsightCache(String existingRdbmsId, boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);
		logger.info("Updating question cache :::: " + cacheable);
		logger.info("Updating question cache minutes :::: " + cacheMinutes);
		logger.info("Updating question cache encrypt :::: " + cacheEncrypt);

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));

		String query = "UPDATE " + TABLE_NAME + " SET "
				+ CACHEABLE_COL+"=?, "
				+ CACHE_MINUTES_COL+"=?, "
				+ CACHE_CRON_COL+"=?, "
				+ CACHED_ON_COL+"=?, "
				+ CACHE_ENCRYPT_COL+"=? WHERE "
				+ QUESTION_ID_COL+"=?";

		PreparedStatement ps = null;
		try {
			ps = insightEngine.getPreparedStatement(query);

			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, cacheable);
			ps.setInt(parameterIndex++, cacheMinutes);
			if(cacheCron == null) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, cacheCron);
			}
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setBoolean(parameterIndex++, cacheEncrypt);
			ps.setString(parameterIndex++, existingRdbmsId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(insightEngine.isConnectionPooling()) {
				try {
					if(ps!=null) {
						ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	public void updateInsightCachedOn(String existingRdbmsId, LocalDateTime cachedOn) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);
		logger.info("Updating question cache date :::: " + cachedOn);

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));

		String query = "UPDATE " + TABLE_NAME + " SET "
				+ CACHED_ON_COL+"=? WHERE "
				+ QUESTION_ID_COL+"=?";

		PreparedStatement ps = null;
		try {
			ps = insightEngine.getPreparedStatement(query);

			int parameterIndex = 1;
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setString(parameterIndex++, existingRdbmsId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(insightEngine.isConnectionPooling()) {
				try {
					if(ps!=null) {
						ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	public void updateInsightGlobal(String existingRdbmsId, boolean isHidden) {
		logger.info("Modifying insight id :::: " + existingRdbmsId);

		String query = "UPDATE " + TABLE_NAME + " SET "
				+ HIDDEN_INSIGHT_COL+"=? WHERE "
				+ QUESTION_ID_COL+"=?";

		PreparedStatement ps = null;
		try {
			ps = insightEngine.getPreparedStatement(query);

			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isHidden);
			ps.setString(parameterIndex++, existingRdbmsId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(insightEngine.isConnectionPooling()) {
				try {
					if(ps!=null) {
						ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
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
