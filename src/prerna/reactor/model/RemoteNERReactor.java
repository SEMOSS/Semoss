package prerna.reactor.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.NamedEntityRecognitionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RemoteNERReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(NERReactor.class);
	
	public RemoteNERReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.PROMPT.getKey(),
				ReactorKeysEnum.ENTITIES.getKey(),
				ReactorKeysEnum.MASK_ENTITIES.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
		};
		this.keyRequired = new int[] {1, 1, 1, 0, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		String prompt = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		List<String> entities = this.getListInput("entities");
		List<String> maskEntities = this.getListInput("maskEntities");

		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		
		return new NounMetadata("Test", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		
	}
	
	
	
	
	
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[4]);
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
	
	private List<String> getListInput(String noun) {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getNoun(noun);
		if (colGRS != null) {
			for (int i = 0; i < colGRS.size(); i++) {
				String stringValue = colGRS.get(i).toString();
				colInputs.add(stringValue);
			}
		}
		return colInputs;
	}

}
