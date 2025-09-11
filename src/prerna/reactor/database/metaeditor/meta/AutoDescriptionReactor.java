package prerna.reactor.database.metaeditor.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.EngineSyncUtility;

public class AutoDescriptionReactor extends AbstractReactor  {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LogManager.getLogger(AutoDescriptionReactor.class);

    public AutoDescriptionReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.DATABASE.getKey(),
            ReactorKeysEnum.ENGINE.getKey()
        };
        this.keyRequired = new int[]{1, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
        String llmEngineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

        try {
            IDatabaseEngine database = Utility.getDatabase(databaseId);
         // 1. Get schema using owlEngine
            AbstractOWLEngine  owlEngine = database.getOWLEngineFactory().getReadOWL();
            Map<String, List<String>> schema = new HashMap<>();
            // Get all concepts (tables)
            List<String> concepts = owlEngine.getConcepts();
            for (String conceptUri : concepts) {
                String tableName = Utility.getInstanceName(conceptUri);
                schema.putIfAbsent(tableName, new ArrayList<>());

                // Get columns for this table
                List<String> propertyUris = owlEngine.getPropertyUris4PhysicalUri(conceptUri);
                for (String propertyUri : propertyUris) {
                    String columnName = Utility.getClassName(propertyUri);
                    schema.get(tableName).add(columnName);
                }
            }

         // 2. Build prompt
            String prompt = """
            	    You are a database documentation assistant.
            	    I will give you a database schema (tables and columns).
            	    You must ALWAYS return JSON following this exact schema:

            	    {
            	      "name": "<table name>",
            	      "description": "<table description>",
            	      "columns": [
            	        {
            	          "name": "<column name>",
            	          "description": "<column description>"
            	        }
            	      ]
            	    }

            	     Rules:
					    - Keys must be EXACTLY: name, description, columns.
					    - Do not use synonyms like 'desc' or 'explanation'.
					    - Only output raw valid JSON (without ```json or ```).
					    - Do not include markdown formatting, explanations, or any text outside JSON.
					    - If multiple tables exist, return a JSON array of table objects.

            	    Here is the schema:

            	    """;

            StringBuilder finalPrompt = new StringBuilder(prompt);

            for (Map.Entry<String, List<String>> entry : schema.entrySet()) {
                finalPrompt.append("Table: ").append(entry.getKey()).append("\n");
                finalPrompt.append("Columns: ").append(String.join(", ", entry.getValue())).append("\n\n");
            }

            // 3. Call LLM
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("temperature", 0.3);
            paramMap.put("max_completion_tokens", 10000);

            IModelEngine modelEngine = Utility.getModel(llmEngineId);
            Map<String, Object> response = modelEngine.ask(
            		finalPrompt.toString(),
                 null,
                this.insight,
                paramMap
            ).toMap();
            
            Object rawResponse = response.get("response");
            List<Map<String, Object>> parsed = new ArrayList<>();

             if (rawResponse instanceof String) {
                String jsonStr = ((String) rawResponse).trim();
                if (jsonStr.startsWith("[")) {
                    // JSON array
                    parsed = mapper.readValue(jsonStr, List.class);
                } else if (jsonStr.startsWith("{")) {
                    // Single JSON object wrap in a list
                    Map<String, Object> parsedMap = mapper.readValue(jsonStr, Map.class);
                    parsed.add(parsedMap);
                } else {
                    throw new IllegalArgumentException("Unexpected JSON format in response: " + jsonStr);
                }
            } else if (rawResponse instanceof Map) {
                // Single object directly
                parsed.add((Map<String, Object>) rawResponse);
            } else {
                throw new RuntimeException("Unexpected response format from LLM: " + rawResponse);
            }

            // 4. Update OWL with all table & column descriptions
            try (WriteOWLEngine writeOwlEngine = database.getOWLEngineFactory().getWriteOWL()) {
                ClusterUtil.pullOwl(databaseId, writeOwlEngine);
                processAllTables(parsed, writeOwlEngine, database);
                writeOwlEngine.export();
                EngineSyncUtility.clearEngineCache(databaseId);
                ClusterUtil.pushOwl(databaseId, writeOwlEngine);
            }

        } catch (Exception e) {
            log.error("Failed to auto-generate descriptions", e);
            NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
            noun.addAdditionalReturn(new NounMetadata("Error generating descriptions", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
            return noun;
        }

        NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
        noun.addAdditionalReturn(new NounMetadata("Successfully generated and added descriptions", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
        return noun;
    }

    @SuppressWarnings("unchecked")
    private void processAllTables(Object parsed, WriteOWLEngine owlEngine, IDatabaseEngine database) throws IOException {
    	if (parsed instanceof List) {
            List<Map<String, Object>> tables = (List<Map<String, Object>>) parsed;
            for (Map<String, Object> tableJson : tables) {
                processTable(tableJson, owlEngine, database);
            }
        } else {
            throw new RuntimeException("Unexpected JSON structure from LLM.");
        }
    }

    //Update OWL for one table and its columns
    @SuppressWarnings("unchecked")
    private void processTable(Map<String, Object> tableJson, WriteOWLEngine owlEngine, IDatabaseEngine database) throws IOException {
        if (tableJson == null || tableJson.isEmpty()) return;

        String tableName = (String) tableJson.get("name");
        if (tableName == null || tableName.isEmpty()) return;

        Map<String, Object> tableDetails = (Map<String, Object>) tableJson;
        if (tableDetails == null) {
            // fallback: use entire map
            tableDetails = new HashMap<>(tableJson);
        }

        // --- Table description ---
        String tableDesc = (String) tableDetails.get("description");
        if (tableDesc != null && !tableDesc.isEmpty()) {
            String tableUri = owlEngine.getPhysicalUriFromPixelSelector(tableName);
            String existingTableDesc = database.getDescription(tableUri);
            if (existingTableDesc != null) {
                owlEngine.deleteDescription(tableUri, existingTableDesc);
            }
            owlEngine.addDescription(tableUri, tableDesc);
        }

        // --- Column descriptions ---
        List<Map<String, Object>> columns = (List<Map<String, Object>>) tableDetails.get("columns");
        if (columns != null) {
            for (Map<String, Object> col : columns) {
                String colName = (String) col.get("name");
                String colDescription = (String) col.get("description");
                if (colName == null || colName.isEmpty()) continue;

                String colUri = owlEngine.getPhysicalUriFromPixelSelector(tableName + "__" + colName);
                String existingColDesc = database.getDescription(colUri);
                if (existingColDesc != null) {
                    owlEngine.deleteDescription(colUri, existingColDesc);
                }
                if (colDescription != null && !colDescription.isEmpty()) {
                    owlEngine.addDescription(colUri, colDescription);
                }
            }
        }
    }
    
    @Override
	public String getReactorDescription() {
		return """
			This reactor creates descriptions for database tables and columns automatically using an LLM model.
        
         Need to pass two keys:
        1. databaseId – The unique identifier for the database.
        2. engineId – The unique identifier for the engine.

        Working Flow:
        - Fetched Database Schema: Retrieves tables and columns from the database using AbstractOWLEngine.
        - Built LLM Prompt: Constructs a structured prompt containing schema details, enforcing a strict JSON response format.
        - Called LLM Model: Uses IModelEngine to auto-generate descriptions for tables and columns based on the schema prompt.
        - Updated OWL file: Applies the generated descriptions to the database by updating the OWL file via WriteOWLEngine.
        
        This ensures that table and column metadata is updated with meaningful descriptions automatically.
        """;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "The id of the database engine to use";
		} else if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Id of the engine";
		} 
		return super.getDescriptionForKey(key);
	}
    
}
