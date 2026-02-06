package prerna.reactor.database;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GetDataProductsReactor extends AbstractReactor {
    
    private static final Logger classLogger = LogManager.getLogger(GetDataProductsReactor.class);
    private static final String MCP_DRIVER_FILE = "py/" + MCPUtility.MCP_PY_FILE_NAME;
    private static final String SMSS_DRIVER_FILE = "py/" + MCPUtility.LEGACY_PY_FILE_NAME;
    
    public GetDataProductsReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
        this.keyRequired = new int[]{1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());

        // Check database access permissions
        if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
            throw new IllegalArgumentException("Database does not exist or user does not have access to database");
        }

        try {
            // Get database engine and its assets folder
            IEngine databaseEngine = Utility.getEngine(databaseId);
            String assetFolder = EngineUtility.getSpecificEngineAssetsFolder(
                databaseEngine.getCatalogType(),
                databaseEngine.getEngineId(),
                databaseEngine.getEngineName()
            );

            // Get data products from Python driver files
            List<Map<String, Object>> dataProducts = getDataProductsFromPythonFiles(assetFolder, databaseId);
            
            classLogger.info("Successfully retrieved " + dataProducts.size() + " data products from database: " + databaseId);
            return new NounMetadata(dataProducts, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
            
        } catch (Exception e) {
            classLogger.error("Error retrieving data products: " + e.getMessage(), e);
            throw new RuntimeException("Failed to retrieve data products: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets data products by reading function names from Python driver files
     * Only returns functions with dataProduct: sql metadata
     */
    private List<Map<String, Object>> getDataProductsFromPythonFiles(String assetFolder, String databaseId) {
        List<Map<String, Object>> dataProducts = new ArrayList<>();
        
        // Check mcp_driver.py first
        Path mcpDriverFile = Paths.get(assetFolder, MCP_DRIVER_FILE);
        if (Files.exists(mcpDriverFile)) {
            List<String> sqlDataProductFunctions = getSqlDataProductFunctions(mcpDriverFile.toString());
            for (String functionName : sqlDataProductFunctions) {
                Map<String, Object> dataProduct = createDataProductFromFunction(functionName, databaseId, "mcp_driver.py");
                dataProducts.add(dataProduct);
            }
        }
        
        // Check smss_driver.py as well (not just fallback)
        Path smssDriverFile = Paths.get(assetFolder, SMSS_DRIVER_FILE);
        if (Files.exists(smssDriverFile)) {
            List<String> sqlDataProductFunctions = getSqlDataProductFunctions(smssDriverFile.toString());
            for (String functionName : sqlDataProductFunctions) {
                Map<String, Object> dataProduct = createDataProductFromFunction(functionName, databaseId, "smss_driver.py");
                dataProducts.add(dataProduct);
            }
        }
        
        return dataProducts;
    }
    
    /**
     * Gets only functions that have dataProduct: sql in their mcp_metadata decorator
     * Uses MCPUtility.getAllFunctionsFromPyFile to get all functions, then filters
     */
    private List<String> getSqlDataProductFunctions(String filePath) {
        List<String> sqlFunctions = new ArrayList<>();
        
        try {
            // Use MCPUtility helper to get all functions
            List<String> allFunctions = MCPUtility.getAllFunctionsFromPyFile(this.insight, filePath);
            
            // Filter functions to only include those with dataProduct: sql metadata
            for (String functionName : allFunctions) {
                if (isSqlDataProductFunction(filePath, functionName)) {
                    sqlFunctions.add(functionName);
                }
            }
        } catch (Exception e) {
            classLogger.error("Error getting SQL data product functions from file: " + filePath, e);
        }
        
        return sqlFunctions;
    }
    
    /**
     * Checks if a function has dataProduct: sql metadata by reading the file
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
     * Creates a data product map from a function name
     */
    private Map<String, Object> createDataProductFromFunction(String functionName, String databaseId, String sourceFile) {
        Map<String, Object> dataProduct = new HashMap<>();
        
        // Use function name as ID (since we don't have separate storage)
        dataProduct.put("id", functionName);
        dataProduct.put("name", humanizeMethodName(functionName));
        dataProduct.put("function_name", functionName);
        dataProduct.put("db_id", databaseId);
        dataProduct.put("source_file", sourceFile);
        dataProduct.put("description", "Data product function: " + humanizeMethodName(functionName));
        
        return dataProduct;
    }
    
    /**
     * Converts a function name to a human-readable format using MCPUtility helper
     */
    private String humanizeMethodName(String functionName) {
        return MCPUtility.formatToTitleCase(functionName);
    }

    @Override
    public String getReactorDescription() {
        return "Retrieve SQL data products (functions with dataProduct: sql metadata) from a database's MCP driver files";
    }
    
    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
            return "The database ID to retrieve data products from";
        }
        return super.getDescriptionForKey(key);
    }
}