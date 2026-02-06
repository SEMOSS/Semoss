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
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class EditDataProductReactor extends AbstractReactor {
    
    private static final Logger classLogger = LogManager.getLogger(EditDataProductReactor.class);
    private static final String MCP_DRIVER_FILE = "py/mcp_driver.py";
    private static final String SMSS_DRIVER_FILE = "py/smss_driver.py";
    
    public EditDataProductReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.DATABASE.getKey(),
            ReactorKeysEnum.ID.getKey(),
            ReactorKeysEnum.NAME.getKey(),
            ReactorKeysEnum.DESCRIPTION.getKey(),
            ReactorKeysEnum.SQL.getKey(),
            ReactorKeysEnum.PARAM_STRUCT.getKey()
        };
        this.keyRequired = new int[]{1, 1, 0, 0, 0, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
        String originalFunctionName = this.keyValue.get(ReactorKeysEnum.ID.getKey()); // Use ID as the original function name
        String newName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
        String newDescription = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());
        String newSqlQuery = this.keyValue.get(ReactorKeysEnum.SQL.getKey());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> newParamStruct = (List<Map<String, Object>>) getList(ReactorKeysEnum.PARAM_STRUCT.getKey());

        // Check database access permissions
        if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
            throw new IllegalArgumentException("Database does not exist or user does not have edit access to database");
        }

        // Validate SQL parameters if SQL is being updated
        if (newSqlQuery != null) {
            int numSqlParams = StringUtils.countMatches(newSqlQuery, "?");
            if (numSqlParams > 0 && (newParamStruct == null || newParamStruct.size() != numSqlParams)) {
                throw new IllegalArgumentException("Number of SQL parameters (" + numSqlParams + ") does not match number of parameter definitions provided (" + 
                    (newParamStruct == null ? 0 : newParamStruct.size()) + ").");
            }
        }

        try {
            // Get database engine and its assets folder
            IEngine databaseEngine = Utility.getEngine(databaseId);
            String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(
                databaseEngine.getCatalogType(),
                databaseEngine.getEngineId(),
                databaseEngine.getEngineName()
            );

            // Update the function in driver files
            boolean updated = updateSqlDataProductInDriverFiles(assetFolder, originalFunctionName, 
                newName, newDescription, newSqlQuery, newParamStruct, databaseId);
            
            if (!updated) {
                throw new IllegalArgumentException("Data product function '" + originalFunctionName + "' not found in driver files or is not a SQL data product");
            }
            
            String updatedName = newName != null ? newName : originalFunctionName;
            classLogger.info("Successfully updated data product function: " + updatedName + " in database: " + databaseId);
            return NounMetadata.getSuccessNounMessage("Data product function '" + updatedName + "' updated successfully");
            
        } catch (Exception e) {
            classLogger.error("Error updating data product: " + e.getMessage(), e);
            throw new RuntimeException("Failed to update data product: " + e.getMessage(), e);
        }
    }
    
    /**
     * Updates the Python function in driver files only if it's a SQL data product
     */
    private boolean updateSqlDataProductInDriverFiles(String assetFolder, String originalFunctionName, 
            String newName, String newDescription, String newSqlQuery, List<Map<String, Object>> newParamStruct, 
            String databaseId) throws IOException {
        
        // Find which driver file contains the SQL data product function
        Path targetFile = findSqlDataProductFile(assetFolder, originalFunctionName);
        
        if (targetFile == null) {
            return false;
        }
        
        // Check for duplicate function name if we're changing the name
        String finalFunctionName = newName != null ? sanitizeFunctionName(newName) : originalFunctionName;
        if (newName != null && !finalFunctionName.equals(originalFunctionName)) {
            if (functionExistsInFiles(assetFolder, finalFunctionName)) {
                throw new IllegalArgumentException("A function with name '" + finalFunctionName + "' already exists");
            }
        }
        
        // Remove old function
        boolean removed = MCPUtility.removeExistingFunctionFromPyFile(this.insight, targetFile.toString(), originalFunctionName);
        if (!removed) {
            return false;
        }
        
        // Create updated data product map
        Map<String, Object> dataProduct = createDataProductMap(
            newName != null ? newName : originalFunctionName,
            newDescription != null ? newDescription : "Updated data product function",
            newSqlQuery != null ? newSqlQuery : "SELECT 1", // Default query if not provided
            newParamStruct,
            databaseId
        );
        
        // Generate new function code
        String newFunctionCode = generatePythonFunction(dataProduct);
        
        // Append new function to the file
        String existingContent = new String(Files.readAllBytes(targetFile));
        String updatedContent = existingContent + "\n\n" + newFunctionCode;
        Files.write(targetFile, updatedContent.getBytes(), 
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            
        classLogger.info("Updated SQL data product function '" + originalFunctionName + "' in " + targetFile.getFileName());
        return true;
    }
    
    /**
     * Finds which driver file contains the specified SQL data product function
     */
    private Path findSqlDataProductFile(String assetFolder, String functionName) {
        // Check mcp_driver.py first
        Path mcpDriverFile = Paths.get(assetFolder, MCP_DRIVER_FILE);
        if (Files.exists(mcpDriverFile) && isSqlDataProductFunction(mcpDriverFile.toString(), functionName)) {
            return mcpDriverFile;
        }
        
        // Check smss_driver.py as fallback
        Path smssDriverFile = Paths.get(assetFolder, SMSS_DRIVER_FILE);
        if (Files.exists(smssDriverFile) && isSqlDataProductFunction(smssDriverFile.toString(), functionName)) {
            return smssDriverFile;
        }
        
        return null;
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
    
    /**
     * Checks if a SQL data product function exists in any driver file
     */
    private boolean functionExistsInFiles(String assetFolder, String functionName) {
        // Check mcp_driver.py
        Path mcpDriverFile = Paths.get(assetFolder, MCP_DRIVER_FILE);
        if (Files.exists(mcpDriverFile) && isSqlDataProductFunction(mcpDriverFile.toString(), functionName)) {
            return true;
        }
        
        // Check smss_driver.py
        Path smssDriverFile = Paths.get(assetFolder, SMSS_DRIVER_FILE);
        if (Files.exists(smssDriverFile) && isSqlDataProductFunction(smssDriverFile.toString(), functionName)) {
            return true;
        }
        
        return false;
    }

    /**
     * Creates a data product map for function generation
     */
    private Map<String, Object> createDataProductMap(String name, String description, String sqlQuery, 
            List<Map<String, Object>> paramStruct, String databaseId) {
        Map<String, Object> dataProduct = new HashMap<>();
        
        dataProduct.put("name", name);
        dataProduct.put("description", description);
        dataProduct.put("db_id", databaseId);
        dataProduct.put("sql_query", sqlQuery);
        
        // Handle parameters
        if (paramStruct != null && !paramStruct.isEmpty()) {
            dataProduct.put("params", paramStruct);
        } else if (sqlQuery != null) {
            // Extract parameters from SQL query if not provided
            dataProduct.put("params", extractParametersFromSql(sqlQuery));
        } else {
            dataProduct.put("params", new ArrayList<>());
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
     * Generates a Python function for the data product (reused from AddDataProductReactor)
     */
    private String generatePythonFunction(Map<String, Object> dataProduct) {
        String name = (String) dataProduct.get("name");
        String description = (String) dataProduct.get("description");
        String databaseId = (String) dataProduct.get("db_id");
        String sqlQuery = (String) dataProduct.get("sql_query");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> params = (List<Map<String, Object>>) dataProduct.get("params");
        
        String functionName = sanitizeFunctionName(name);
        StringBuilder function = new StringBuilder();
        
        // Function decorator with dataProduct metadata
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
            function.append("    df = db.execQuery(f\"").append(pythonSql).append("\")\n");
        } else {
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
        return "Edit a SQL data product function (with dataProduct: sql metadata) in a database's MCP driver files";
    }
    
    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
            return "The database ID containing the data product function to edit";
        } else if (key.equals(ReactorKeysEnum.ID.getKey())) {
            return "The original name/ID of the Python function to edit";
        } else if (key.equals(ReactorKeysEnum.NAME.getKey())) {
            return "New name for the data product function (optional)";
        } else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
            return "New description for the data product function (optional)";
        } else if (key.equals(ReactorKeysEnum.SQL.getKey())) {
            return "New SQL query for the data product function (optional)";
        } else if (key.equals(ReactorKeysEnum.PARAM_STRUCT.getKey())) {
            return "New parameter structure defining the SQL query parameters (optional)";
        }
        return super.getDescriptionForKey(key);
    }
}