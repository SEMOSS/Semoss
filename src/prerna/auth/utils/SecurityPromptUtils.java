package prerna.auth.utils;

import java.io.IOException;
import java.sql.Clob;
import java.sql.PreparedStatement;
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
	
	private final static String promptQuery = "INSERT INTO PROMPT (ID, TITLE, CONTEXT, CREATED_BY, DATE_CREATED, IS_PUBLIC) "
			+ "VALUES (?, ?, ?, ?, ?, ?)";
	
	private final static String promptInputQuery = "INSERT INTO PROMPT_INPUT (ID, PROMPT_ID, INDEX, KEY, DISPLAY, TYPE, IS_HIDDEN_PHRASE_INPUT_TOKEN, LINKED_INPUT_TOKEN) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	
	private final static String promptVaraibleQuery = "INSERT INTO PROMPT_VARIABLE (ID, PROMPT_ID, PROMPT_INPUT_ID, TYPE, META) "
			+ "VALUES (?, ?, ?, ?, ?)";
	
	private final static String promptMetaQuery = "INSERT INTO PROMPT_VARIABLE (ID, PROMPT_ID, PROMPT_INPUT_ID, TYPE, META) "
			+ "VALUES (?, ?, ?, ?, ?)";
	
	private final static List<String> PROMPT_COLUMNS = Arrays.asList(
			"ID",
			"TITLE",
			"CONTEXT",
			"CREATED_BY",
			"DATE_CREATED",
			"IS_PUBLIC"
			);
	
	private final static List<String> PROMPT_INPUT_COLUMNS = Arrays.asList(
			"ID",
			"PROMPT_ID",
			"INDEX",
			"KEY",
			"DISPLAY",
			"TYPE",
			"IS_HIDDEN_PHRASE_INPUT_TOKEN",
			"LINKED_INPUT_TOKEN"
			);
	
	private final static List<String> PROMPT_VARIABLE_COLUMNS = Arrays.asList(
			"ID",
			"PROMPT_ID",
			"PROMPT_INPUT_ID",
			"TYPE",
			"META",
			"VALUE"
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
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPT__IS_PUBLIC", "==", true));
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
		appendPromptInputs(promptDetails, listIndexPromptMapping, promptIdList);
		appendPromptVariables(promptDetails, listIndexPromptMapping, promptIdList);
		appendPromptTags(promptDetails, listIndexPromptMapping, promptIdList);
		return promptDetails;
		
	}
	
	/**
	 * Main Function to add in prompt
	 * Handles validation for every required input 
	 * Inserts into PROMPT, PROMPT_INPUT, PROMPT_VARIABLE, PROMPTMETA, PROMPTMETAKEYS, PROMPTPERMISSION
	 * @param promptDetails
	 * @param userId
	 */
	public static void addPrompt(Map<String, Object> promptDetails, String userId) {
		List<Map<String, Object>> inputs = (List<Map<String, Object>>) promptDetails.get("inputs");
		Map<String, Map<String, Object>> inputTypes = (Map<String, Map<String, Object>>) promptDetails.get("inputTypes");
		boolean allowClob = securityDb.getQueryUtil().allowClobJavaObject();
		
		List<String> tags = (List<String>) promptDetails.get("tags");
		Boolean isFavorite = (Boolean) promptDetails.get("favorite");
		String promptId = UUID.randomUUID().toString();
		
		promptDeatilsValidation(promptDetails);
		
		insertPrompt(promptDetails, userId, allowClob, promptId);
		Map<Integer, String> indexPromptMap = insertPromptInputs(inputs, promptId);
		insertPromptVariables(inputTypes, promptId, indexPromptMap);
		insertTags(tags, promptId);
		
		insertPromptPermission(userId, isFavorite, promptId);
		
	}
	
	/**
	 * HELPER FUNCTIONS FOR CREATING RETURN FOR LIST OF PROMPTS
	 */
	
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
	 * Queries PromptInput Table and appends correct data structure to retList 
	 * @param promptDetails
	 * @param listIndexPromptMapping
	 * @param promptIdList
	 */
	private static void appendPromptInputs(List<Map<String, Object>> promptDetails,
			Map<String, Integer> listIndexPromptMapping, List<String> promptIdList) {
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String pic : PROMPT_INPUT_COLUMNS) {
			qs.addSelector(new QueryColumnSelector(PROMPT_INPUT + "__" + pic));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(PROMPT_INPUT + "__PROMPT_ID", "==", promptIdList));
		qs.addOrderBy(PROMPT_INPUT + "__PROMPT_ID");
		qs.addOrderBy(PROMPT_INPUT + "__INDEX");
		// Loop through get tags 
		
		List<Map<String, Object>> retList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		for(Map<String, Object> ret: retList) {
			String promptId = (String) ret.get("PROMPT_ID");
			Integer loc = listIndexPromptMapping.get(promptId);
			List<Map<String, Object>> inputList = (List<Map<String, Object>>) promptDetails.get(loc).get("inputs");
			if(inputList == null) {
				inputList = new ArrayList<>();
			}
			inputList.add(ret);
			promptDetails.get(loc).put("inputs", inputList);
		}
	}

	/**
	 * Queries and Appends inputTypes from PROMPT_VARIABLES table 
	 * @param promptDetails
	 * @param listIndexPromptMapping
	 * @param promptIdList
	 */
	private static void appendPromptVariables(List<Map<String, Object>> promptDetails,
			Map<String, Integer> listIndexPromptMapping, List<String> promptIdList) {
		SelectQueryStruct qs = new SelectQueryStruct();
		for (String piv : PROMPT_VARIABLE_COLUMNS) {
			qs.addSelector(new QueryColumnSelector(PROMPT_VARIABLE + "__" + piv));
		}
		qs.addSelector(new QueryColumnSelector(PROMPT_INPUT + "__INDEX"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(PROMPT_VARIABLE + "__PROMPT_ID", "==", promptIdList));
		qs.addOrderBy(PROMPT_VARIABLE + "__PROMPT_ID");
		qs.addOrderBy(PROMPT_VARIABLE + "__INDEX");
		qs.addRelation(PROMPT_VARIABLE + "__" + "PROMPT_INPUT_ID", PROMPT_INPUT + "__" + "ID", "left.join");
		// Loop through get tags 
		
		List<Map<String, Object>> retList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		for(Map<String, Object> ret: retList) {
			String promptId = (String) ret.get("PROMPT_ID");
			String promptInputIndex = String.valueOf((Integer)ret.get("INDEX"));
			Integer loc = listIndexPromptMapping.get(promptId);
			Map<String, Map<String, Object>> inputTypeMap = (Map<String, Map<String, Object>>) promptDetails.get(loc).get("inputTypes");
			if(inputTypeMap == null) {
				inputTypeMap = new HashMap<>();
			}
			inputTypeMap.put(promptInputIndex, ret);
			promptDetails.get(loc).put("inputTypes", inputTypeMap);
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
			qs.addSelector(new QueryColumnSelector(PROMPT + "__" + pc));
		}
		qs.addSelector(new QueryColumnSelector("PROMPTP__FAVORITE"));
		
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			subQs.addSelector(new QueryColumnSelector("PROMPTPERMISSION__FAVORITE"));
			subQs.addSelector(new QueryColumnSelector("PROMPTPERMISSION__PROMPT_ID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROMPTPERMISSION__USERID", "==", userId));
			qs.addRelation(new SubqueryRelationship(subQs, "PROMPTP", "left.outer.join", 
					new String[] {PROMPT + "__ID", "PROMPTP__PROMPT_ID", "="}));

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
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(PROMPT+"__IS_PUBLIC", "==", true));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(PROMPT+"__CREATED_BY", "==", userId));
		qs.addExplicitFilter(orFilter);
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
		validatePromptInput((List<Map<String, Object>>) promptDetails.get("inputs"));
		
		Map<String, Map<String, Object>> inputTypes = (Map<String, Map<String, Object>>) promptDetails.get("inputTypes");
		List<String> tags = (List<String>) promptDetails.get("tags");
		
		if(inputTypes != null && !inputTypes.isEmpty()) {
			validatePromptInputVariables(inputTypes);
		}
		
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

	private static void validatePromptInputVariables(Map<String, Map<String, Object>> inputVariables) {
		for(Map.Entry<String, Map<String, Object>> entry: inputVariables.entrySet()) {
			Map<String, Object> inputTypes = entry.getValue();
			validateString(inputTypes, "type", false, false);
			validateString(inputTypes, "meta", true, false);
		}
		
	}

	private static void validatePromptInput(List<Map<String, Object>> inputs) {
		for(Map<String, Object> inputInfo: inputs) {
			validateInteger(inputInfo, "index");
			validateString(inputInfo, "key", false, false);
			validateString(inputInfo, "display", false, false);
			validateString(inputInfo, "type", false, false);
			defaultToFalse(inputInfo, "is_hidden_phrase_input_token");
			validateString(inputInfo, "linked_input_token", true, false);
		}
	}


	private static void validatePromptBaseDetails(Map<String, Object> promptDetails) {
		// TODO Auto-generated method stub
		validateString(promptDetails, "title", false, false);
		validateString(promptDetails, "context", false, false);
		defaultToFalse(promptDetails, "is_public");
		
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
	 * Inserts prompt input types into the Prompt Variables table. 
	 * @param inputTypes
	 * @param promptId
	 * @param indexPromptMap
	 */
	private static void insertPromptVariables(Map<String, Map<String, Object>> inputTypes, String promptId,
			Map<Integer, String> indexPromptMap) {
		PreparedStatement promptVaraiblesPS = null;
		try {
			promptVaraiblesPS = securityDb.getPreparedStatement(promptVaraibleQuery);
			for(Map.Entry<String, Map<String, Object>> entry: inputTypes.entrySet()) {
				int index = 1;
				Integer promptIndex = Integer.valueOf(String.valueOf(entry.getKey()));
				String promptInputId = indexPromptMap.get(promptIndex);
				String meta = (String) entry.getValue().get("meta");
				String type = (String) entry.getValue().get("type");
//				String value = (String) entry.getValue().get("VALUE");
				
				promptVaraiblesPS.setString(index++, UUID.randomUUID().toString());
				promptVaraiblesPS.setString(index++, promptId);
				promptVaraiblesPS.setString(index++, promptInputId);
				if(meta != null && !meta.isEmpty()) {
					promptVaraiblesPS.setString(index++, meta);
				} else {
					promptVaraiblesPS.setNull(index++, java.sql.Types.VARCHAR);
				}
				promptVaraiblesPS.setString(index++, type);
				promptVaraiblesPS.addBatch();
			}
			
			promptVaraiblesPS.executeBatch();
			if (!promptVaraiblesPS.getConnection().getAutoCommit()) {
				promptVaraiblesPS.getConnection().commit();
			}
			
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, promptVaraiblesPS, null);
		}
	}

	/**
	 * Inserts prompt inputs per prompt into PromptInput Table
	 * @param inputs
	 * @param promptId
	 * @return
	 */
	private static Map<Integer, String> insertPromptInputs(List<Map<String, Object>> inputs, String promptId) {
		PreparedStatement promptInputPS = null;
		Map<Integer, String> indexPromptMap = new HashMap<>();
		try {
			promptInputPS = securityDb.getPreparedStatement(promptInputQuery);
			for(Map<String, Object> inputDetails: inputs) {
				String promptInputId = UUID.randomUUID().toString();
				int index = 1;
				promptInputPS.setString(index++, promptInputId);
				promptInputPS.setString(index++, promptId);
				Integer promptIndex = (Integer) inputDetails.get("index");
				promptInputPS.setInt(index++, promptIndex);
				promptInputPS.setString(index++, String.valueOf(inputDetails.get("key")));
				promptInputPS.setString(index++, String.valueOf(inputDetails.get("display")));
				promptInputPS.setString(index++, String.valueOf(inputDetails.get("type")));
				Boolean value = (Boolean) inputDetails.get("is_hidden_phrase_input_token");
				promptInputPS.setBoolean(index++, value);
				String linkedInputToken = (String) inputDetails.get("linked_input_token");
				if(linkedInputToken != null) {
					promptInputPS.setString(index++, linkedInputToken);
				} else {
					promptInputPS.setNull(index++, java.sql.Types.VARCHAR);
				}
				indexPromptMap.put(promptIndex, promptInputId);
				promptInputPS.addBatch();
			}
			promptInputPS.executeBatch();
			if (!promptInputPS.getConnection().getAutoCommit()) {
				promptInputPS.getConnection().commit();
			}
			
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("ERROR FOR NOW");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, null, promptInputPS, null);
		}
		return indexPromptMap;
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
			promptPS.setString(index++, userId);
			promptPS.setTimestamp(index++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
			promptPS.setBoolean(index++, Boolean.valueOf((boolean) promptDetails.get("is_public")));
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
	
}
