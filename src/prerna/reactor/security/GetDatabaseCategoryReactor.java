package prerna.reactor.security;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.DatabaseCategoryEnum;

/**
 * Reactor that determines the category (SQL or NoSQL) of a database engine
 * Takes an engine ID and returns the database category
 */
public class GetDatabaseCategoryReactor extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(GetDatabaseCategoryReactor.class);

    
    public GetDatabaseCategoryReactor() {
        this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
    }
    
    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
        
        if(engineId == null || engineId.isEmpty()) {
            throw new IllegalArgumentException("Must input an engine id");
        }
        
        User user = this.insight.getUser();
        
        // Validate engine access and get engine ID
        engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);
        if(!SecurityEngineUtils.userCanViewEngine(user, engineId) && 
           !SecurityEngineUtils.engineIsDiscoverable(engineId)) {
            throw new IllegalArgumentException("Engine does not exist or user does not have access to the database");
        }
        
        DatabaseCategoryEnum category = determineDatabaseCategory(engineId);
        
        return new NounMetadata(category.getCategoryName(), PixelDataType.CONST_STRING, PixelOperationType.ENGINE_INFO);
        
	}
    
    /**
     * Determines the database category using multiple approaches for robustness
     * @param engineId The engine ID
     * @return DatabaseCategoryEnum (SQL or NOSQL)
     */
    private DatabaseCategoryEnum determineDatabaseCategory(String engineId) {
       //get engine metadata and check RDBMS_TYPE (subtype) from SMSS
        try {
            NounMetadata engineMetadataResult = getEngineMetadata(engineId);
            
            classLogger.info("Retrieved engine metadata for engine ID: {}", engineId);
            classLogger.info("Engine metadata: {}", engineMetadataResult);

            if (engineMetadataResult != null && engineMetadataResult.getValue() instanceof Map) {
                Map<String, Object> engineMetadata = (Map<String, Object>) engineMetadataResult.getValue();
                Object rdbmsType = engineMetadata.get("database_subtype");
                classLogger.info("rdbms type: {}", rdbmsType);
                if (rdbmsType != null) {
                    return DatabaseCategoryEnum.getCategoryFromRdbmsType(rdbmsType.toString());
                }
            }            
        } catch (Exception e) {
        }
        
        return DatabaseCategoryEnum.UNKNOWN;
    }

    private NounMetadata getEngineMetadata(String engineId) {
        GenRowStruct engineGrs = this.store.makeNoun(ReactorKeysEnum.ENGINE.getKey());
        engineGrs.add(new NounMetadata(engineId, PixelDataType.CONST_STRING));
        
        GetEngineMetadataReactor metadataReactor = new GetEngineMetadataReactor();
        metadataReactor.setInsight(this.insight);
        metadataReactor.setNounStore(this.store);
        
        return metadataReactor.execute();
    }
}