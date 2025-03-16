package prerna.reactor.security;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class UpdateEmbeddingsReactor extends AbstractSetMetadataReactor {
    private static final Logger classLogger = LogManager.getLogger(UpdateEmbeddingsReactor.class);

    public UpdateEmbeddingsReactor() {
        this.keysToGet = new String[]{
        		ReactorKeysEnum.ENGINE.getKey(),
        		ReactorKeysEnum.NEW_EMBEDDER.getKey()};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = UploadInputUtility.getEngineNameOrId(this.store);
        engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
        
        if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
            throw new IllegalArgumentException("Engine does not exist or user does not have access to edit");
        }

        // Retrieve the new embedder engine ID from metadata
        Map<String, Object> metadata = getMetaMap();
        List<String> newEmbedderEngineIdList = (List<String>) metadata.get("new_embedder");
        String newEmbedderEngineId = (newEmbedderEngineIdList != null && !newEmbedderEngineIdList.isEmpty()) ? newEmbedderEngineIdList.get(0) : null;
        if (newEmbedderEngineId == null || newEmbedderEngineId.trim().isEmpty()) {
            throw new IllegalArgumentException("New embedder engine ID cannot be null or empty");
        }

        updateEmbeddings(engineId, this.insight, newEmbedderEngineId);
        updateEmbedderInSMSS(engineId, newEmbedderEngineId);
        
        NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
        noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully updated embeddings for the new model"));
        return noun;
    }

    private void updateEmbeddings(String engineId, Insight insight, String newEmbedderEngineId) {
        IVectorDatabaseEngine vectorDatabase = Utility.getVectorDatabase(engineId);
        if (vectorDatabase == null) {
            throw new RuntimeException("Unable to find vector database for engine: " + engineId);
        }
        if (!vectorDatabase.userCanAccessEmbeddingModels(this.insight.getUser())) {
            throw new IllegalArgumentException("User does not have access to vector database dependent models");
        }
        
        Map<String, Object> paramMap = getMap();
	    if (paramMap == null) {
	        paramMap = new HashMap<>();
	    }
	    paramMap.put(AbstractVectorDatabaseEngine.INSIGHT, this.insight);
        
        // Call the new method to update embeddings
        vectorDatabase.recalculateEmbeddings(newEmbedderEngineId, insight, paramMap);
    }

    private void updateEmbedderInSMSS(String engineId, String newEmbedderEngineId) {
        String smssFile = (String) DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
        Map<String, String> keyToNewValue = new HashMap<>();
        keyToNewValue.put(Constants.EMBEDDER_ENGINE_ID, newEmbedderEngineId);

        IModelEngine newEmbeddingModel = Utility.getModel(newEmbedderEngineId);
        if (newEmbeddingModel == null) {
            throw new RuntimeException("Failed to load new embedding model: " + newEmbedderEngineId);
        }

        String newEmbedderAlias = newEmbeddingModel.getSmssProp().getProperty(Constants.ENGINE_ALIAS);
        keyToNewValue.put(Constants.EMBEDDER_ENGINE_NAME, newEmbedderAlias);
        
        try {
            Utility.changePropertiesFileValue(smssFile, keyToNewValue, false);
        } catch (IOException e) {
            classLogger.error("Failed to update SMSS file", e);
        }
    }
    
    /**
	 * Get the map from the paramValues noun store
	 * @return list of engines to delete
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		
		return null;
	}

    @Override
    public String getReactorDescription() {
        return "Recalculates embeddings for existing content using a new embedding model and updates metadata.";
    }
}
