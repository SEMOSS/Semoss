package prerna.auth.utils;

import java.io.IOException;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

public class SecurityPromptUtils extends AbstractSecurityUtils {
	
	private static Logger classLogger = LogManager.getLogger(SecurityPromptUtils.class);
	private final static String PROMPT = "PROMPT";
	private final static String PROMPT_INPUT = "PROMPT_INPUT";
	private final static String PROMPT_VARIABLE = "PROMPT_VARIABLE";
	
	private final static String promptQuery = "INSERT INTO PROMPT (ID, TITLE, CONTEXT, VERSION, INTENT, CREATED_BY, DATE_CREATED, IS_LATEST) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	
//	private final static String promptInputQuery = "INSERT INTO PROMPT_INPUT (ID, PROMPT_ID, INDEX, KEY, DISPLAY, TYPE, IS_HIDDEN_PHRASE_INPUT_TOKEN, LINKED_INPUT_TOKEN) "
//			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
//	
//	private final static String promptVaraibleQuery = "INSERT INTO PROMPT_VARIABLE (ID, PROMPT_ID, PROMPT_INPUT_ID, TYPE, META) "
//			+ "VALUES (?, ?, ?, ?, ?)";
	
	private final static String promptMetaQuery = "INSERT INTO PROMPT_VARIABLE (ID, PROMPT_ID, PROMPT_INPUT_ID, TYPE, META) "
			+ "VALUES (?, ?, ?, ?, ?)";
	
	private final static List<String> PROMPT_COLUMNS = Arrays.asList(
			"ID",
			"TITLE",
			"CONTEXT",
			"VERSION",
			"INTENT"
,			"CREATED_BY",
			"DATE_CREATED",
			"IS_LATEST"
			);

	
	
	/**
	 * MAIN PROMPT REACTOR FUNCTIONS 
	 */
	
	/**
	 * Returns a boolean after querying the Prompt table to see if a public prompt with the input title exsists. 
	 * @param promptTitle
	 * @return
	 */
	public static Boolean checkPromptTitle(String promptTitle) {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPT__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__TITLE", "==", promptTitle));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}
	
	/**
	 * Returns a list of Prompts that are created by the signed in user or are public 
	 * Formatted to be a Map that includes the following
	 * inputs -> List<Map<String, Object>>
	 * inputTypes -> Map<String, Map<String, Object>>
	 * tags -> List<String> 
	 * @param userId
	 * @param filters
	 * @param promptMetadataFilter
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getPrompts(String userId, GenRowFilters filters, Map<String, Object> promptMetadataFilter, String limit, String offset) {

		List<Map<String, Object>> promptDetails = appendPromptInfo(userId, filters, promptMetadataFilter, limit, offset);
		Map<String, Integer> listIndexPromptMapping = new HashMap<>();
		List<String> promptIdList = new ArrayList<>();
		Integer i = 0;
		for(Map<String, Object> prompt: promptDetails) {
			String promptId = (String) prompt.get("ID");
			promptIdList.add(promptId);
			listIndexPromptMapping.put(promptId, i++);
		}
		
		appendPromptTags(promptDetails, listIndexPromptMapping, promptIdList);
		return promptDetails;
		
	}
	
	/**
	 * Main Function to add in prompt
	 * Handles validation for every required input 
	 * Inserts into PROMPT, PROMPTMETA, PROMPTMETAKEYS, PROMPTPERMISSION
	 * @param promptDetails
	 * @param userId
	 */
	public static void addPrompt(Map<String, Object> promptDetails, String userId) {
		boolean allowClob = securityDb.getQueryUtil().allowClobJavaObject();
		
		List<String> tags = (List<String>) promptDetails.get("tags");
		
		String promptId = UUID.randomUUID().toString();
		
		promptDeatilsValidation(promptDetails);
		
		insertPrompt(promptDetails, userId, allowClob, promptId);
		insertTags(tags, promptId);
		
		
	}
	
	public static void editPrompt(Map<String, Object> promptDetails, String userId) {
		// TODO Auto-generated method stub
		boolean allowClob = securityDb.getQueryUtil().allowClobJavaObject();
		
		List<String> tags = (List<String>) promptDetails.get("tags");
		
		String promptId = (String) promptDetails.get("id");
		
		promptDeatilsValidation(promptDetails);
		updatePrompt(promptId);
		insertPrompt(promptDetails, userId, allowClob, promptId);
	}
	
	/**
	 * HELPER FUNCTIONS FOR CREATING RETURN FOR LIST OF PROMPTS
	 */
	
	private static void updatePrompt(String promptId) {
		String[] colToUpdate = {"IS_LATEST"};
		String[] whereCol = {"ID"};
		String promptPermissionQuery = securityDb.getQueryUtil().createUpdatePreparedStatementString("PROMPT", colToUpdate, whereCol);
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(promptPermissionQuery);
			int i = 1;
			ps.setBoolean(i++, false);
			ps.setString(i++, promptId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Queries PROMPTMETA and creates the correct formatted return 
	 * @param promptDetails
	 * @param listIndexPromptMapping
	 * @param promptIdList
	 */
	private static void appendPromptTags(List<Map<String, Object>> promptDetails,
			Map<String, Integer> listIndexPromptMapping, List<String> promptIdList) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__METAORDER"));
		qs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID"));
		
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__PROMPT_ID", "==", promptIdList));
		qs.addOrderBy("PROMPTMETA__PROMPT_ID");
		qs.addOrderBy("PROMPTMETA__METAORDER");
		// Loop through get tags 
		
		List<Map<String, Object>> retList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		for(Map<String, Object> ret: retList) {
			String promptId = (String) ret.get("PROMPT_ID");
			String tag = (String) ret.get("METAVALUE");
			Integer loc = listIndexPromptMapping.get(promptId);
			List<String> tagList = (List<String>) promptDetails.get(loc).get("tags");
			if(tagList == null) {
				tagList = new ArrayList<>();
			}
			tagList.add(tag);
			promptDetails.get(loc).put("tags", tagList);
		}
	}

	
	
	/**
	 * Queries and appends Prompt info from PROMPT table
	 * @param userId
	 * @param filters
	 * @param promptMetadataFilter
	 * @param limit
	 * @param offset
	 * @return
	 */
	private static List<Map<String, Object>> appendPromptInfo(String userId, GenRowFilters filters, Map<String, Object> promptMetadataFilter, String limit, String offset) {
		// QUERY PROMPT get ID, TITLE, CONTEXT, IS Public, other small thigngs 
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String pc : PROMPT_COLUMNS) {
			if(pc != "IS_LATEST") {
				qs.addSelector(new QueryColumnSelector(PROMPT + "__" + pc));
			}
		}


		if(promptMetadataFilter != null && !promptMetadataFilter.isEmpty()) {
			for(String k: promptMetadataFilter.keySet()) {
				SelectQueryStruct subMetaQs = new SelectQueryStruct();
				subMetaQs.addSelector(new QueryColumnSelector("PROMPTMETA__PROMPT_ID"));
				subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAKEY", "==", k));
				subMetaQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTMETA__METAVALUE", "==", promptMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROMPT__ID", "==", subMetaQs));
			}
		}

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));

		if(filters != null && !filters.isEmpty()) {
			qs.mergeExplicitFilters(filters);
		}
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
			qs.setLimit(long_limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
			qs.setOffSet(long_offset);
		}
		
		
		List<Map<String, Object>> promptDetails = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return promptDetails;
	}
	

	/**
	 * HELPER FUNCTIONS FOR INPUT VALIDATION WHEN ADDING PROMPT 
	 * 
	 */
	
	private static void promptDeatilsValidation(Map<String, Object> promptDetails) {
		
		validatePromptBaseDetails(promptDetails);
		List<String> tags = (List<String>) promptDetails.get("tags");

		if (tags != null && !tags.isEmpty()) {
			validatePromptTags(tags);
		}
		
	}

	private static void validatePromptTags(List<String> tags) {
		// TODO Auto-generated method stub
		for(String tag: tags) {
			if(tag == null || tag.isEmpty()) {
				throw new IllegalArgumentException("Tag must be string and not empty");
			}
		}
	}



	private static void validatePromptBaseDetails(Map<String, Object> promptDetails) {
		// TODO Auto-generated method stub
		validateString(promptDetails, "title", false, false);
		validateString(promptDetails, "context", false, false);

		
	}
	
	
	private static void validateInteger(Map<String, Object> promptDetails, String mapKey) {
		// TODO Auto-generated method stub
		Integer value = null;
		try {
			value = (Integer) promptDetails.get(mapKey);
			if(value == null) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Prompt");
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
		
	}

	private static void defaultToFalse(Map<String, Object> promptDetails, String mapKey) {
		// TODO Auto-generated method stub
		Boolean value = null;
		try {
			value = (Boolean) promptDetails.get(mapKey);
			if(value == null) {
				promptDetails.put(mapKey, false);
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
		 
	}

	private static void validateString(Map<String, Object> promptDetails, String mapKey, boolean nullable, boolean allowEmpty) {
		// TODO Auto-generated method stub
		String value = null;
		try {
			value = (String) promptDetails.get(mapKey);
			value = value != null ? value.trim(): value;
			if(value == null && !nullable) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Prompt");
			}
			if(value != null && value.isEmpty() && !allowEmpty) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Prompt");
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
		
	}
	
	/**
	 * HELPER METHODS FOR INSERT PROMPT INTO ALL SEPERATE TABLES 
	 */
	
	/**
	 * Inserts Prompt info about user favorites into PROMPTPERMISSION Table. 
	 * @param userId
	 * @param isFavorite
	 * @param promptId
	 */
	private static void insertPromptPermission(String userId, Boolean isFavorite, String promptId) {
		String promptPermissionQuery = securityDb.getQueryUtil().createInsertPreparedStatementString("PROMPTPERMISSION",
				new String[] { "PROMPT_ID", "USERID", "FAVORITE", "DATEADDED" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(promptPermissionQuery);

			int parameterIndex = 1;
			ps.setString(parameterIndex++, promptId);
			ps.setString(parameterIndex++, userId);
			ps.setBoolean(parameterIndex++, isFavorite);
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			ps.addBatch();
			
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Inserts Prompt Tags per prompt into the PROMPTMETA table 
	 * @param tags
	 * @param promptId
	 */
	private static void insertTags(List<String> tags, String promptId) {
		// now we do the new insert with the order of the tags
		String promptMetaQuery = securityDb.getQueryUtil().createInsertPreparedStatementString("PROMPTMETA",
				new String[] { "PROMPT_ID", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(promptMetaQuery);
			int i = 0;
			for (String tag : tags) {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, promptId);
					ps.setString(parameterIndex++, "tag");
					ps.setString(parameterIndex++, tag);
					ps.setInt(parameterIndex++, i++);
					ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	

	/**
	 * Inserts basic prompt details in to Prompt table. 
	 * Basic details include - title, context, is_public, date_created, id, created_by
	 * @param promptDetails
	 * @param userId
	 * @param allowClob
	 * @param promptId
	 */
	private static void insertPrompt(Map<String, Object> promptDetails, String userId, boolean allowClob,
			String promptId) {
		PreparedStatement promptPS = null;
		try {
			promptPS = securityDb.getPreparedStatement(promptQuery);
			int index = 1;
			promptPS.setString(index++, promptId);
			promptPS.setString(index++, String.valueOf(promptDetails.get("title")));
			if(allowClob) {
				Clob toclob = securityDb.getConnection().createClob();
				toclob.setString(1,  String.valueOf(promptDetails.get("context")));
				promptPS.setClob(index++, toclob);
			} else {
				promptPS.setString(index++, String.valueOf(promptDetails.get("context")));
			}
			// Get version of exisiting prompt
			Integer version = getVersionNumber(promptId);
			promptPS.setInt(index++, version);
			promptPS.setString(index++, String.valueOf(promptDetails.get("intent")));
			promptPS.setString(index++, userId);
			promptPS.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			promptPS.setBoolean(index++, true);
			promptPS.execute();
			if (!promptPS.getConnection().getAutoCommit()) {
				promptPS.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, promptPS, null);
		}
	}

	private static Integer getVersionNumber(String promptId) {
		Integer version = 0;
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROMPT__VERSION"));
		qs.addSelector(new QueryColumnSelector("PROMPT__DATE_CREATED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__ID", "==", promptId));
		qs.addOrderBy("PROMPT__DATE_CREATED", "desc");
		qs.setLimit(1);
//		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_LATEST", "==", true));
		

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
//				System.out.println((Integer) wrapper.next().getValues()[0]);
				version = (Integer) wrapper.next().getValues()[0];
				version+=1;
				return version;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return version;
	}

	public static void deletePrompt(String promptId) {
		List<String> deletes = new ArrayList<>();
		deletes.add("DELETE FROM PROMPT WHERE ID=?");
		deletes.add("DELETE FROM PROMPTMETA WHERE PROMPT_ID=?");
		
		for(String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, promptId);
				ps.execute();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}


	
}
