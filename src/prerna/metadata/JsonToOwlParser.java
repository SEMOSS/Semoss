package prerna.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JsonToOwlParser {
    private static final Logger classLogger = LogManager.getLogger(JsonToOwlParser.class);

    /**
     * Parses the given JSON string and converts the data elements into OWL format.
     *
     * @param json      
     * @param databaseId 
     * @throws Exception 
     */
    public void parseJsonToOwl(String json, String databaseId) throws Exception {
        ObjectMapper jsonMapper = new ObjectMapper();
        JsonNode rootNode = jsonMapper.readTree(json);

        // Extract table data from JSON structure
        Map<String, Map<String, String>> tableData = extractTableData(rootNode);

        // Write extracted data to OWL format
        writeToOwl(tableData, databaseId);
    }

    /**
     * Extracts the table name, column name, and data type from the JSON structure.
     *
     * @param rootNode 
     * @return 
     */
	private Map<String, Map<String, String>> extractTableData(JsonNode rootNode) {
		Map<String, Map<String, String>> tableData = new HashMap<>();

		JsonNode dataElementsNode = rootNode.path("Out_DC_DA_DE").path("Data_Collection").path("Data_Assets")
				.path("objects").path("Data_Elements");

		for (JsonNode elementNode : dataElementsNode) {
			String tableName = elementNode.path("Table").asText();
			String columnName = elementNode.path("name").asText();
			String dataType = elementNode.path("DataType").asText();

			// Initialize table map if not already present
			tableData.putIfAbsent(tableName, new HashMap<>());

			// Add column details to the corresponding table
			tableData.get(tableName).put(columnName, dataType);
		}

		return tableData;
	}

    /**
     * Converts the collected table data into OWL format and writes it using the OWL engine.
     *
     * @param tableData  
     * @param databaseId 
     * @throws Exception 
     */
    private void writeToOwl(Map<String, Map<String, String>> tableData, String databaseId) throws Exception {
        IDatabaseEngine databaseEngine = Utility.getDatabase(databaseId);

        try (WriteOWLEngine owlWriter = databaseEngine.getOWLEngineFactory().getWriteOWL()) {
            for (Map.Entry<String, Map<String, String>> tableEntry : tableData.entrySet()) {
                String tableName = tableEntry.getKey();
                owlWriter.addConcept(tableName);

                for (Map.Entry<String, String> columnEntry : tableEntry.getValue().entrySet()) {
                    String columnName = columnEntry.getKey();
                    String columnDataType = columnEntry.getValue();

                    classLogger.info("Adding table: {} with column: {} (type: {})", tableName, columnName, columnDataType);
                    owlWriter.addProp(tableName, columnName, columnDataType);
                }
            }

            // Export the OWL file
            owlWriter.export();
        }
    }
}
