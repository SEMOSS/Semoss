package prerna.reactor.database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class AddDataProductReactor extends AbstractReactor {
    
    private static final Logger classLogger = LogManager.getLogger(AddDataProductReactor.class);
    private static final String MCP_DRIVER_FILE = "py/mcp_driver.py";
    
    public AddDataProductReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.DATABASE.getKey(), 
            ReactorKeysEnum.SQL.getKey(), 
            ReactorKeysEnum.NAME.getKey(),
            ReactorKeysEnum.DESCRIPTION.getKey(),
            ReactorKeysEnum.PARAM_STRUCT.getKey()
        };
        this.keyRequired = new int[]{1, 1, 1, 1, 0};
    }

	@Override
	public NounMetadata execute() {
        organizeKeys();
        String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
        String sqlQuery = this.keyValue.get(ReactorKeysEnum.SQL.getKey());
        String name = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
        String description = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> paramStruct = (List<Map<String, Object>>) getList(ReactorKeysEnum.PARAM_STRUCT.getKey());

        // Validate SQL parameters using paramStruct
        int numSqlParams = StringUtils.countMatches(sqlQuery, "?");
        if (numSqlParams > 0 && (paramStruct == null || paramStruct.size() != numSqlParams)) {
            throw new IllegalArgumentException("Number of SQL parameters (" + numSqlParams + ") does not match number of parameter definitions provided (" + 
                (paramStruct == null ? 0 : paramStruct.size()) + ").");
        }

        // Check database access permissions
        if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
            throw new IllegalArgumentException("Database does not exist or user does not have edit access to database");
        }

        try {
            // Get database engine and its assets folder
            IEngine databaseEngine = Utility.getEngine(databaseId);
            String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(
                databaseEngine.getCatalogType(),
                databaseEngine.getEngineId(),
                databaseEngine.getEngineName()
            );

            // Create data product entry
            Map<String, Object> dataProduct = createDataProduct(name, description, databaseId, sqlQuery, paramStruct);
            
            // Check if function already exists
            String functionName = sanitizeFunctionName(name);
            if (functionExistsInFiles(assetFolder, functionName)) {
                throw new IllegalArgumentException("A data product function with name '" + functionName + "' already exists");
            }
            
            // Create or update MCP driver file (no JSON file needed)
            updateMcpDriverFile(assetFolder, dataProduct);
            
            classLogger.info("Successfully added data product: " + name + " to database assets: " + databaseId);
            return NounMetadata.getSuccessNounMessage("Data product '" + name + "' added successfully to database assets");
            
        } catch (Exception e) {
            classLogger.error("Error adding data product: " + e.getMessage(), e);
            throw new RuntimeException("Failed to add data product: " + e.getMessage(), e);
        }
	}
	
	/**
	 * Creates a data product map with all necessary fields
	 */
	private Map<String, Object> createDataProduct(String name, String description, String databaseId, 
	        String sqlQuery, List<Map<String, Object>> paramStruct) {
	    Map<String, Object> dataProduct = new HashMap<>();
	    
	    // Generate unique ID for data product
	    String id = java.util.UUID.randomUUID().toString();
	    
	    dataProduct.put("id", id);
	    dataProduct.put("name", name);
	    dataProduct.put("description", description);
	    dataProduct.put("db_id", databaseId);
	    dataProduct.put("sql_query", sqlQuery);
	    dataProduct.put("mcp_included", true);  // Default to true
	    dataProduct.put("mcp_exec", "auto");    // Default execution mode
	    dataProduct.put("mcp_loc", "inline");   // Default location
	    dataProduct.put("created_date", System.currentTimeMillis());
	    
	    // Handle parameters
	    if (paramStruct != null && !paramStruct.isEmpty()) {
	        dataProduct.put("params", paramStruct);
	    } else {
	        // Extract parameters from SQL query if not provided
	        dataProduct.put("params", extractParametersFromSql(sqlQuery));
	    }
	    
	    return dataProduct;
	}
	
	/**
	 * Extracts parameter placeholders from SQL query and creates parameter structure
	 */
	private List<Map<String, Object>> extractParametersFromSql(String sqlQuery) {
	    List<Map<String, Object>> params = new ArrayList<>();
	    
	    // Pattern to match SQL parameter placeholders like :paramName or ?
	    Pattern pattern = Pattern.compile(":(\\w+)|\\?");
	    Matcher matcher = pattern.matcher(sqlQuery);
	    int paramIndex = 1;
	    
	    while (matcher.find()) {
	        Map<String, Object> param = new HashMap<>();
	        String paramName;
	        
	        if (matcher.group(1) != null) {
	            // Named parameter like :paramName
	            paramName = matcher.group(1);
	        } else {
	            // Positional parameter ?
	            paramName = "param" + paramIndex;
	        }
	        
	        param.put("name", paramName);
	        param.put("description", "Parameter for " + paramName);
	        param.put("type", "string");  // Default type
	        param.put("testValue", "");   // Empty test value
	        
	        params.add(param);
	        paramIndex++;
	    }
	    
	    return params;
	}
	
	/**
	 * Creates or updates the MCP driver file with the new data product function
	 */
	private void updateMcpDriverFile(String assetFolder, Map<String, Object> dataProduct) throws IOException {
	    Path mcpDriverFile = Paths.get(assetFolder, MCP_DRIVER_FILE);
	    Path pyDirectory = mcpDriverFile.getParent();
	    
	    // Ensure py directory exists
	    Files.createDirectories(pyDirectory);
	    
	    // Create __init__.py file if it doesn't exist to make it a proper Python package
	    Path initFile = pyDirectory.resolve("__init__.py");
	    if (!Files.exists(initFile)) {
	        Files.write(initFile, "# MCP Driver Package\n".getBytes(), 
	            StandardOpenOption.CREATE);
	    }
	    
	    String functionCode = generatePythonFunction(dataProduct);
	    
	    if (Files.exists(mcpDriverFile)) {
	        // Read existing file and append new function
	        String existingContent = new String(Files.readAllBytes(mcpDriverFile));
	        
	        // Check if function already exists
	        String functionName = sanitizeFunctionName((String) dataProduct.get("name"));
	        if (!existingContent.contains("def " + functionName + "(")) {
	            String updatedContent = existingContent + "\n\n" + functionCode;
	            Files.write(mcpDriverFile, updatedContent.getBytes(), 
	                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
	            classLogger.info("Appended function '" + functionName + "' to existing MCP driver file");
	        } else {
	            classLogger.info("Function '" + functionName + "' already exists in MCP driver file, skipping");
	        }
	    } else {
	        // Create new file with header and function
	        String header = generateMcpDriverHeader();
	        String fullContent = header + "\n\n" + functionCode;
	        Files.write(mcpDriverFile, fullContent.getBytes(), 
	            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
	        classLogger.info("Created new MCP driver file with function '" + sanitizeFunctionName((String) dataProduct.get("name")) + "'");
	    }
	}
	
	/**
	 * Generates the header for the MCP driver file
	 */
	private String generateMcpDriverHeader() {
	    return "from semoss import Insight\n" +
	           "from ai_server import DatabaseEngine\n" +
	           "import json\n" +
	           "from smssutil import mcp_metadata\n\n" +
	           "def _response(data):\n" +
	           "    # Double encode to ensure safe transport to JS\n" +
	           "    final_output_str = json.dumps(data, separators=(',', ':'))\n" +
	           "    return json.dumps(final_output_str, separators=(',', ':'))";
	}
	
	/**
	 * Generates a Python function for the data product
	 */
	private String generatePythonFunction(Map<String, Object> dataProduct) {
	    String name = (String) dataProduct.get("name");
	    String description = (String) dataProduct.get("description");
	    String databaseId = (String) dataProduct.get("db_id");
	    String sqlQuery = (String) dataProduct.get("sql_query");
	    @SuppressWarnings("unchecked")
	    List<Map<String, Object>> params = (List<Map<String, Object>>) dataProduct.get("params");
	    
	    String functionName = sanitizeFunctionName(name);
	    StringBuilder function = new StringBuilder();        // Function decorator
        function.append("@mcp_metadata({'execution': 'auto', 'displayLocation': 'inline', 'dataProduct': 'sql'})\n");
	    
	    // Function signature
	    function.append("def ").append(functionName).append("(");
	    if (params != null && !params.isEmpty()) {
	        for (int i = 0; i < params.size(); i++) {
	            if (i > 0) function.append(", ");
	            String paramName = (String) params.get(i).get("name");
	            String paramType = (String) params.get(i).getOrDefault("type", "string");
	            
	            // Add type hints
	            String pythonType = mapToPythonType(paramType);
	            function.append(paramName).append(": ").append(pythonType);
	        }
	    }
	    function.append("):\n");
	    
	    // Function docstring
	    function.append("    \"\"\"").append(description.replace("\"", "\\\"")).append("\"\"\"\n");
	    
	    // Function body - handle list parameter parsing if needed
	    boolean hasListParams = params != null && params.stream()
	        .anyMatch(p -> "list".equalsIgnoreCase((String) p.getOrDefault("type", "string")));
	    
	    if (hasListParams) {
	        function.append("    import json\n");
	        for (Map<String, Object> param : params) {
	            if ("list".equalsIgnoreCase((String) param.getOrDefault("type", "string"))) {
	                String paramName = (String) param.get("name");
	                function.append("    # Parse list parameter if it's a JSON string\n");
	                function.append("    if isinstance(").append(paramName).append(", str):\n");
	                function.append("        ").append(paramName).append(" = json.loads(").append(paramName).append(")\n");
	            }
	        }
	        function.append("\n");
	    }
	    
	    function.append("    db = DatabaseEngine(engine_id=\"").append(databaseId).append("\")\n");
	    
	    // Build SQL execution with parameters
	    if (params != null && !params.isEmpty()) {
	        // Convert ? placeholders to Python f-string format
	        String pythonSql = convertSqlToFString(sqlQuery, params);
	        function.append("    df = db.execQuery(f\"").append(pythonSql).append("\")\n");    } else {
        function.append("    df = db.execQuery(\"").append(sqlQuery.replace("\"", "\\\"")).append("\")\n");
    }
	    
	    function.append("    return _response(df.to_dict(orient='records'))");
	    
	    return function.toString();
	}
	
	/**
	 * Converts SQL with ? placeholders to Python f-string format
	 */
	private String convertSqlToFString(String sql, List<Map<String, Object>> params) {
	    String result = sql;
	    
	    // Replace ? placeholders with {param_name} format, handling different parameter types
	    int paramIndex = 0;
	    while (result.contains("?") && paramIndex < params.size()) {
	        String paramName = (String) params.get(paramIndex).get("name");
	        String paramType = (String) params.get(paramIndex).getOrDefault("type", "string");
	        
	        String replacement;
	        if ("list".equalsIgnoreCase(paramType)) {
	            // For list parameters, build the tuple for IN clauses
	            replacement = "\" + str(tuple(item.upper() for item in " + paramName + ")) + \"";
	        } else if ("string".equalsIgnoreCase(paramType) || "text".equalsIgnoreCase(paramType)) {
	            // For string parameters, use UPPER() for case insensitivity
	            replacement = "'{" + paramName + ".upper()}'";
	        } else {
	            // For numeric parameters, no quotes needed
	            replacement = "{" + paramName + "}";
	        }
	        
	        result = result.replaceFirst("\\?", replacement);
	        paramIndex++;
	    }
	    
	    // Make string comparisons case insensitive by wrapping column names in UPPER()
	    result = makeCaseInsensitive(result);
	    
	    return result;
	}
	
	/**
	 * Sanitizes function name to be valid Python identifier
	 */
	private String sanitizeFunctionName(String name) {
	    return name.toLowerCase()
	               .replaceAll("[^a-zA-Z0-9_]", "_")
	               .replaceAll("_{2,}", "_")
	               .replaceAll("^_|_$", "");
	}

    @Override
    public String getReactorDescription() {
        return "Add a SQL data product to a database's assets folder with SQL query and parameters, creating MCP driver functions with dataProduct: sql metadata";
    }
    
    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
            return "The database ID to query for the data product and where MCP driver files will be stored";
        } else if (key.equals(ReactorKeysEnum.SQL.getKey())) {
            return "The SQL query for the data product";
        } else if (key.equals(ReactorKeysEnum.NAME.getKey())) {
            return "Name of the data product";
        } else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
            return "Description of the data product";
        } else if (key.equals(ReactorKeysEnum.PARAM_STRUCT.getKey())) {
            return "Parameter structure defining the SQL query parameters with name, description, type, and testValue fields (optional)";
        }
        return super.getDescriptionForKey(key);
    }
    
    /**
	 * Maps parameter types to Python type hints
	 */
	private String mapToPythonType(String paramType) {
	    if (paramType == null) {
	        return "str";
	    }
	    
	    switch (paramType.toLowerCase()) {
	        case "string":
	        case "text":
	        case "varchar":
	            return "str";
	        case "int":
	        case "integer":
	        case "bigint":
	            return "int";
	        case "float":
	        case "double":
	        case "decimal":
	        case "numeric":
	            return "float";
	        case "boolean":
	        case "bool":
	            return "bool";
	        case "list":
	        case "array":
	            return "list";
	        case "date":
	        case "datetime":
	        case "timestamp":
	            return "str";  // Date strings for simplicity
	        default:
	            return "str";  // Default to string
	    }
	}
	
	/**
	 * Makes SQL queries case insensitive by wrapping column names in UPPER()
	 */
	private String makeCaseInsensitive(String sql) {
	    // Simple approach: wrap column names before = and IN with UPPER()
	    sql = sql.replaceAll("(\\w+)\\s*(=)\\s*'\\{([^}]+)\\}'", "UPPER($1) $2 '{$3}'");
	    sql = sql.replaceAll("(\\w+)\\s+(IN)\\s+", "UPPER($1) $2 ");
	    
	    return sql;
	}
	
	/**
     * Checks if a SQL data product function exists in any driver file
     */
    private boolean functionExistsInFiles(String assetFolder, String functionName) throws IOException {
        // Check mcp_driver.py
        Path mcpDriverFile = Paths.get(assetFolder, MCP_DRIVER_FILE);
        if (Files.exists(mcpDriverFile) && isSqlDataProductFunction(mcpDriverFile.toString(), functionName)) {
            return true;
        }
        
        // Check smss_driver.py
        Path smssDriverFile = Paths.get(assetFolder, "py/smss_driver.py");
        if (Files.exists(smssDriverFile) && isSqlDataProductFunction(smssDriverFile.toString(), functionName)) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Checks if a function exists in the file and has dataProduct: sql metadata
     */
    private boolean isSqlDataProductFunction(String filePath, String functionName) {
        try {
            List<String> fileLines = Files.readAllLines(Paths.get(filePath));
            boolean hasDataProductSql = false;
            
            for (String line : fileLines) {
                String trimmedLine = line.trim();
                
                // Check for mcp_metadata decorator with dataProduct: sql
                if (trimmedLine.startsWith("@mcp_metadata(") && trimmedLine.contains("'dataProduct': 'sql'")) {
                    hasDataProductSql = true;
                }
                
                // Check for function definition
                if (trimmedLine.startsWith("def " + functionName + "(")) {
                    return hasDataProductSql;
                }
                
                // Reset if we hit another function without finding our target
                if (trimmedLine.startsWith("def ") && !trimmedLine.startsWith("def " + functionName + "(")) {
                    hasDataProductSql = false;
                }
            }
        } catch (Exception e) {
            classLogger.error("Error checking function in file: " + filePath, e);
        }
        
        return false;
    }

}