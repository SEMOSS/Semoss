package prerna.reactor.database;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

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

public class DeleteDataProductReactor extends AbstractReactor {
    
    private static final Logger classLogger = LogManager.getLogger(DeleteDataProductReactor.class);
    private static final String MCP_DRIVER_FILE = "py/mcp_driver.py";
    private static final String SMSS_DRIVER_FILE = "py/smss_driver.py";
    
    public DeleteDataProductReactor() {
        this.keysToGet = new String[]{
            ReactorKeysEnum.DATABASE.getKey(), 
            ReactorKeysEnum.NAME.getKey()
        };
        this.keyRequired = new int[]{1, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
        String functionName = this.keyValue.get(ReactorKeysEnum.NAME.getKey());

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

            // Remove the function from driver files
            boolean removed = removeSqlDataProductFromDriverFiles(assetFolder, functionName);
            
            if (!removed) {
                throw new IllegalArgumentException("Data product function '" + functionName + "' not found in driver files or is not a SQL data product");
            }
            
            classLogger.info("Successfully deleted data product function: " + functionName + " from database: " + databaseId);
            return NounMetadata.getSuccessNounMessage("Data product function '" + functionName + "' deleted successfully");
            
        } catch (Exception e) {
            classLogger.error("Error deleting data product: " + e.getMessage(), e);
            throw new RuntimeException("Failed to delete data product: " + e.getMessage(), e);
        }
    }
    
    /**
     * Removes the Python function from MCP driver files only if it's a SQL data product
     */
    private boolean removeSqlDataProductFromDriverFiles(String assetFolder, String functionName) {
        // Try to remove from mcp_driver.py first
        Path mcpDriverFile = Paths.get(assetFolder, MCP_DRIVER_FILE);
        if (Files.exists(mcpDriverFile)) {
            if (isSqlDataProductFunction(mcpDriverFile.toString(), functionName)) {
                try {
                    boolean removed = MCPUtility.removeExistingFunctionFromPyFile(
                        this.insight, mcpDriverFile.toString(), functionName);
                    if (removed) {
                        classLogger.info("Removed SQL data product function '" + functionName + "' from mcp_driver.py");
                        return true;
                    }
                } catch (Exception e) {
                    classLogger.warn("Failed to remove function from mcp_driver.py: " + e.getMessage());
                }
            }
        }
        
        // Try to remove from smss_driver.py as fallback
        Path smssDriverFile = Paths.get(assetFolder, SMSS_DRIVER_FILE);
        if (Files.exists(smssDriverFile)) {
            if (isSqlDataProductFunction(smssDriverFile.toString(), functionName)) {
                try {
                    boolean removed = MCPUtility.removeExistingFunctionFromPyFile(
                        this.insight, smssDriverFile.toString(), functionName);
                    if (removed) {
                        classLogger.info("Removed SQL data product function '" + functionName + "' from smss_driver.py");
                        return true;
                    }
                } catch (Exception e) {
                    classLogger.warn("Failed to remove function from smss_driver.py: " + e.getMessage());
                }
            }
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

    @Override
    public String getReactorDescription() {
        return "Delete a SQL data product function (with dataProduct: sql metadata) from a database's MCP driver files";
    }
    
    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.DATABASE.getKey())) {
            return "The database ID from which to delete the data product function";
        } else if (key.equals(ReactorKeysEnum.NAME.getKey())) {
            return "The name of the Python function to delete";
        }
        return super.getDescriptionForKey(key);
    }
}