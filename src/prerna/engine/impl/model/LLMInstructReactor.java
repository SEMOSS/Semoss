package prerna.engine.impl.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class LLMInstructReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(LLMInstructReactor.class);
	
	public LLMInstructReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), 
				ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.CONTEXT.getKey(), 
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()};
		this.keyRequired = new int[] {1, 1, 0, 0};
	}
	
	List<Map<String, Object>> getProjectData() {
		User user = this.insight.getUser();
		try {
		List<Map<String, Object>> projectInfo = SecurityProjectUtils.getUserProjectDescriptions(user);  

        // Create the keySet of project ids
        if(!projectInfo.isEmpty()) {
        	Map<String, Integer> index = new HashMap<>(projectInfo.size());
        	int size = projectInfo.size();
        	for(int i = 0; i < size; i++) {
        		Map<String, Object> project = projectInfo.get(i);
        		String projectId = project.get("project_id").toString();
        		index.put(projectId, Integer.valueOf(i));
        	}
        
        IRawSelectWrapper wrapper = null;
        List<String> metaKeys = Arrays.asList("description");
        
        try {
        	wrapper = SecurityProjectUtils.getProjectMetadataWrapper(index.keySet(), metaKeys, true);
        	while(wrapper.hasNext()) {
        		Object[] data = wrapper.next().getValues();
        		String projectId = (String) data[0];
        		String metaKey = (String) data[1];
        		String metaValue = (String) data[2];
        		if (metaValue == null) {
        			continue;
        		}
				int indexToFind = index.get(projectId);
				Map<String, Object> res = projectInfo.get(indexToFind);
				// whatever it is, if it is single send a single value, if it is multi send as array
				if(res.containsKey(metaKey)) {
					Object obj = res.get(metaKey);
					if(obj instanceof List) {
						((List) obj).add(metaValue);
					} else {
						List<Object> newList = new ArrayList<>();
						newList.add(obj);
						newList.add(metaValue);
						res.put(metaKey, newList);
					}
				} else {
					res.put(metaKey, metaValue);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					
				}
			}
		}
        }


        return projectInfo;
        
		} catch (Exception e) {
			classLogger.error("There was a problem fetching the user project descriptions", e);
			return Collections.emptyList();
		}
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		List<Map<String, Object>> projectData = this.getProjectData();

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Model " + engineId + " does not exist or user does not have access to this model");
		}
		
		String task = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		String context = this.keyValue.get(this.keysToGet[2]);
		if (context != null) {
			context = Utility.decodeURIComponent(context);
		}
		
		Map<String, Object> paramMap = getMap();
		IModelEngine modelEngine = Utility.getModel(engineId);
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		Map<String, Object> output = modelEngine.instruct(task, context, projectData, this.insight, paramMap).toMap();
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[3]);
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
