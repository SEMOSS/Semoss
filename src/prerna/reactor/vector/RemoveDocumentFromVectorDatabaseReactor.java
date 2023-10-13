package prerna.reactor.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class RemoveDocumentFromVectorDatabaseReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(RemoveDocumentFromVectorDatabaseReactor.class);

	public RemoveDocumentFromVectorDatabaseReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), "fileNames", ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1, 1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this engine");
		}

		List<String> fileNames = getFiles();
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);
		try {
			eng.removeDocument(fileNames, paramMap);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			String errorMessage = e.getMessage();
			
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);	
	}

	/**
	 * Get inputs
	 * @return list of engines to delete
	 */
	public List<String> getFiles() {
		List<String> filePaths = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				filePaths.add(grs.get(i).toString());
			}
			return filePaths;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			filePaths.add(this.curRow.get(i).toString());
		}
		return filePaths;
	}
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[2]);
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
}
