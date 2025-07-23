package prerna.reactor.database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class QueryDatabaseReactor extends AbstractReactor {
	
  private static Logger logger = LogManager.getLogger(QueryDatabaseReactor.class);
  private static final Gson gson = new Gson();

  public QueryDatabaseReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
    this.keyRequired = new int[] {1, 1, 1, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    
    // check database permissions
    String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
    if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
		throw new IllegalArgumentException(
				"Database " + databaseId + " does not exist or user does not have access to this database");
	}
    
    // check model permissions
    String engine = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
    if (!SecurityEngineUtils.userCanViewEngine(user, engine)) {
		throw new IllegalArgumentException(
				"Model " + engine + " does not exist or user does not have access to this model");
	}
    IRDBMSEngine database = (RDBMSNativeEngine) Utility.getDatabase(databaseId);
    IModelEngine modelEngine = Utility.getModel(engine);
    	
    // get relation info about the database
    List<String> concepts = database.getPixelConcepts();
    Map<String, Object[]> metamodel = database.getMetamodel();
    Object[] edges = metamodel.get("edges");
    
    Map<String, Object> conceptInfo = new HashMap<>();
    List<Map<String, String>> edgeInfo = new ArrayList<>();
    for (Object edge : edges) {
    	Map<String, String> e = (Map<String, String>) edge;
    	if (e.containsKey("rel")) {
    		String[] splitRel = e.get("rel").split("\\.");
        	if (splitRel.length == 4) {
        		Map<String, String> relInfo = new HashMap<>();
            	relInfo.put("source_table", splitRel[0]);
            	relInfo.put("source_table_column", splitRel[1]);
            	relInfo.put("target_table", splitRel[2]);
            	relInfo.put("target_table_column", splitRel[3]);
            	edgeInfo.add(relInfo);
        	} else {
        		logger.warn("Could not determine relation for " + e.get("rel"));
        	}
    	}
    }
    
    // get descriptions, logical names, and relation info for columns and put in map
    for (String concept : concepts) {
    	List<String> properties = database.getPixelSelectors(concept);
    	String databaseUri = database.getPhysicalUriFromPixelSelector(concept);
    	String description = database.getDescription(databaseUri);
    	Map<String, Object> propertyMap = new HashMap<>();
    	for (String property : properties) {
    		String physicalUri = database.getPhysicalUriFromPixelSelector(property);
    		String desc = database.getDescription(physicalUri);
    		Set<String> logicalNames = database.getLogicalNames(physicalUri);
    		Map<String, Object> dataMap = new HashMap<>();
    		if (desc != null && !desc.trim().equals("")) {
    			dataMap.put("description", desc);
    		}
    		if (!logicalNames.isEmpty()) {
    			dataMap.put("logical_names", logicalNames);
    		}
    		for (Map<String, String> relInfo : edgeInfo) {
    			if (relInfo.get("target_table").equals(concept)) {
    				String p = database.getConceptualName(physicalUri);
    				if (relInfo.get("target_table_column").equals(p)) {
    					dataMap.put("foreign_key", relInfo);
    				}
    			}
    		}
    		String parsedProperty = property.split("__")[1];
    		propertyMap.put(parsedProperty, dataMap);
    	}
    	Map<String, Object> conceptJson = new HashMap<>();
    	if (description != null && !description.trim().equals("")) {
    		conceptJson.put("description", description);
    	}
    	conceptJson.put("columns", propertyMap);
    	conceptInfo.put(concept, conceptJson);
    }
        
    // generate context for llm
    String sqlContext =
		"""
		You are an expert SQL assistant. You are provided with a database schema in JSON format, which includes tables, table-level descriptions, columns, and metadata such as descriptions, logical names, and foreign key relationships.
		Your task is to generate an SQL query that answers the user's question, using only the information explicitly available in the schema.
		
		Return your answer as a JSON string with the following fields:
		"question": The user's question, verbatim.
		"sql": The SQL query that answers the question, or an empty string if the schema does not provide enough information to generate the query.
		"explanation": A brief explanation of how you mapped the question to the schema and constructed the query, or why the query could not be generated.
		
		Instructions:
		1. Carefully read and interpret the user's question. Restate your understanding of what the user is asking for to yourself.
		2. Identify which tables and columns in the schema are relevant to answering the question, using only information explicitly present in the schema (including descriptions, logical names, and foreign key metadata).
		3. Generate the SQL query using only SELECT statements. Do not use INSERT, UPDATE, or DELETE.
		4. Only join tables if a foreign key relationship is defined in the schema. Do not assume relationships that are not explicitly specified.
		5. Do not use any columns, tables, or relationships that are not present or described in the schema.
		6. The SQL query should be valid and as efficient as possible.
		7. The SQL query should be compatible for a %s database.
		7. If you use a column or table based on its description or logical name, briefly explain your reasoning in the "explanation" field.
		8. Before finalizing your answer, think step-by-step about what result your SQL query will produce, and check if it matches the user's request.
		9. If the SQL query does not exactly answer the user's question, or if there is not enough information in the schema, leave the "sql" field blank and explain why in the "explanation" field.
		10. Make sure the JSON string can be parsed with GSON.
		
		Schema:
		%s
		
		Your output must be a plain JSON string with the keys: "question", "sql", and "explanation".
		Do not wrap your response in any formatting or code blocks.
		""";

    String context = String.format(sqlContext, database.getDbType(), gson.toJson(conceptInfo));
    String prompt = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
    Map<String, Object> paramMap = getParamMap();
    if (paramMap == null) {
		paramMap = new HashMap<String, Object>();
	}
    
    // add response format to ensure json schema
    // still need to make sure this works
    paramMap.put("response_format", getJsonSchema());
    
    // ask model and parse response
    Map<String, Object> queryResponse = modelEngine.ask(prompt, context, this.insight, paramMap).toMap();
    String responseString = (String) queryResponse.get("response");
    String cleanedResponse = responseString.trim().replace("\\n", "").replace("\\\"", "\"");
    Map<String, Object> responseMap = parseResponse(cleanedResponse);
    if (responseMap == null || !responseMap.containsKey("question") || !responseMap.containsKey("sql") || !responseMap.containsKey("explanation")) {
    	throw new SemossPixelException("LLM could not generate proper response");
    }
    
    if (responseMap.get("sql") == null || ((String) responseMap.get("sql")).trim().isEmpty()) {
    	return new NounMetadata(responseMap, PixelDataType.MAP);
    }
    
    // TODO connect to db and add query to prepared statement, get results and return to FE
    Connection con = null;
	try {
		con = database.makeConnection();
		String sql = (String) responseMap.get("sql");
		System.out.println(sql);
	    
	    try (PreparedStatement ps = con.prepareStatement(sql)) {
	    	ResultSet rs = ps.executeQuery();
	    	
	    	// I can't find the Wrapper manager way of converting a result set to a map
	    	ResultSetMetaData rsmd = rs.getMetaData();
	    	List<Map<String, String>> columnInfo = new ArrayList<>();
	        int columnCount = rsmd.getColumnCount();
	        
	    	List<Map<String, Object>> resultObject = new ArrayList<>();
	    	boolean gotMetadata = false;
	    	while (rs.next()) {
	    		Map<String, Object> m = new HashMap<>();
	    		int columnIndex = 1;
	    		while (columnIndex < columnCount + 1) {
	    			if (!gotMetadata) {
	    				Map<String, String> col = new HashMap<>();
	    				col.put("name", rsmd.getColumnName(columnIndex));
	    				col.put("type", rsmd.getColumnName(columnIndex));
	    				columnInfo.add(col);
	    			}
	    			
	    			m.put(rsmd.getColumnName(columnIndex), rs.getObject(columnIndex++));
	    		}
	    		gotMetadata = true;
	    		resultObject.add(m);
	    	}
	    	responseMap.put("metadata", columnInfo);
	    	responseMap.put("result_set", resultObject);
	    	return new NounMetadata(responseMap, PixelDataType.MAP);
	    } catch (SQLException e) {
	    	throw new SemossPixelException("Could not run generated SQL");
	    }
	} catch (Exception e) {
		throw new IllegalArgumentException("Error occured establishing connection to database: " + e.getMessage());
	} finally {
		try {
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}    
  }
  
  private Map<String, Object> getParamMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}
  
  private Map<String, Object> parseResponse(String jsonString) {
      Type type = new TypeToken<Map<String, Object>>(){}.getType();
      Map<String, Object> map = null;

      try {
          map = gson.fromJson(jsonString, type);
      } catch (JsonSyntaxException e) {
    	  logger.error("Failed to parse JSON response");
          throw new SemossPixelException("Failed to parse JSON response: " + jsonString, e);
      }
      return map;
  }
  
  private Map<String, Object> getJsonSchema() {
	  Map<String, Object> questionProp = new HashMap<>();
      questionProp.put("type", "string");

      Map<String, Object> sqlProp = new HashMap<>();
      sqlProp.put("type", "string");

      Map<String, Object> explanationProp = new HashMap<>();
      explanationProp.put("type", "string");

      Map<String, Object> properties = new HashMap<>();
      properties.put("question", questionProp);
      properties.put("sql", sqlProp);
      properties.put("explanation", explanationProp);

      List<String> required = Arrays.asList("question", "sql", "explanation");

      Map<String, Object> schema = new HashMap<>();
      schema.put("type", "object");
      schema.put("properties", properties);
      schema.put("required", required);
      schema.put("additionalProperties", false);

      Map<String, Object> jsonSchema = new HashMap<>();
      jsonSchema.put("name", "sql_generator");
      jsonSchema.put("schema", schema);
      jsonSchema.put("strict", true);
      
      Map<String, Object> paramJson = new HashMap<>();
      paramJson.put("type", "json_schema");
      paramJson.put("json_schema", jsonSchema);
      return paramJson;
  }
}
