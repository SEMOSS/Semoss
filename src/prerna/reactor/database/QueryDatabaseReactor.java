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

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class QueryDatabaseReactor extends AbstractReactor {

  public QueryDatabaseReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
    this.keyRequired = new int[] {1, 1, 1, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    
    String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
    if (!SecurityEngineUtils.userCanViewEngine(user, databaseId)) {
		throw new IllegalArgumentException(
				"Database " + databaseId + " does not exist or user does not have access to this database");
	}
    
    String engine = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
    if (!SecurityEngineUtils.userCanViewEngine(user, engine)) {
		throw new IllegalArgumentException(
				"Model " + engine + " does not exist or user does not have access to this model");
	}
    IDatabaseEngine database = Utility.getDatabase(databaseId);
    IModelEngine modelEngine = Utility.getModel(engine);
	
    List<String> concepts = database.getPixelConcepts();
    Map<String, Object[]> metamodel = database.getMetamodel();
    Object[] edges = metamodel.get("edges");
    
    Map<String, Object> conceptInfo = new HashMap<>();
    List<Map<String, String>> edgeInfo = new ArrayList<>();
    for (Object edge : edges) {
    	Map<String, String> e = (Map<String, String>) edge;
    	String[] splitRel = e.get("rel").split("\\.");
    	Map<String, String> relInfo = new HashMap<>();
    	relInfo.put("source_table", splitRel[0]);
    	relInfo.put("source_table_column", splitRel[1]);
    	relInfo.put("target_table", splitRel[2]);
    	relInfo.put("target_table_column", splitRel[3]);
    	edgeInfo.add(relInfo);
    }
    
    
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
    		propertyMap.put(property, dataMap);
    	}
    	Map<String, Object> conceptJson = new HashMap<>();
    	if (description != null && !description.trim().equals("")) {
    		conceptJson.put("description", description);
    	}
    	conceptJson.put("columns", propertyMap);
    	conceptInfo.put(concept, conceptJson);
    }
    
    String sqlContext = 
    	"""
		You are an expert SQL assistant. You are provided with a database schema in JSON format, which includes tables, table-level descriptions, columns, and metadata such as descriptions, logical names, and foreign key relationships.
		Your task is to generate an SQL query that answers the user's question, using only the information explicitly available in the schema.
		Return your answer as a JSON string with the following fields:
		
		"question": The user's question, verbatim.
		"sql": The SQL query that answers the question, or an empty string if the schema does not provide enough information to generate the query.
		"explanation": A brief explanation of how you mapped the question to the schema and constructed the query, or why the query could not be generated.
		
		Instructions:
		
		Only use columns and tables that are present in the provided schema.
		Only join tables if a foreign key relationship is defined in the schema. Do not assume relationships that are not explicitly specified.
		You may use column descriptions, logical names, and foreign key metadata to infer the meaning and appropriate usage of columns.
		If the user's question requires information not available in the schema, leave the "sql" field blank and explain why in the "explanation" field.
		Do not use any columns, tables, or relationships that are not present or described in the schema.
		The SQL query should be valid and as efficient as possible.
		If you use a column or table based on its description or logical name, briefly explain your reasoning in the "explanation" field.
		Do not surround your response in a code block and make sure the JSON string can be parsed with GSON.
		
		Schema:
		%s
    	""";

    String context = String.format(sqlContext, conceptInfo.toString());
    String prompt = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
    Map<String, Object> paramMap = getParamMap();
    if (paramMap == null) {
		paramMap = new HashMap<String, Object>();
	}
    paramMap.put("response_format", getJsonSchema());
    
    Map<String, Object> queryResponse = modelEngine.ask(prompt, context, this.insight, paramMap).toMap();
    String responseString = (String) queryResponse.get("response");
    Map<String, String> responseMap = parseResponse(responseString);
    if (responseMap == null) {
    	throw new SemossPixelException("LLM could not generate proper response");
    }
    
    return new NounMetadata(responseMap, PixelDataType.MAP);
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
  
  private Map<String, String> parseResponse(String jsonString) {
	  Gson gson = new Gson();
      Type type = new TypeToken<Map<String, String>>(){}.getType();
      Map<String, String> map = null;

      try {
          map = gson.fromJson(jsonString, type);
      } catch (JsonSyntaxException e) {
          System.err.println("Failed to parse JSON: " + e.getMessage());
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
