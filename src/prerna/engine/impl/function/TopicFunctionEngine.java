package prerna.engine.impl.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class TopicFunctionEngine extends AbstractFunctionEngine2 {

	// allow a particular topic or not
	
	private static final Logger classLogger = LogManager.getLogger(TopicFunctionEngine.class);
	String llmEngine = null;
	
	public TopicFunctionEngine()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.INPUT.getKey(), ReactorKeysEnum.TOPIC_MAP.getKey()};
		this.keyRequired = new int[] {1, 1};
	}
	
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// input - is the input - typically this is an array
		// get the topic map
		// call the specified llm to get the topics
		// get the topics and their outputs
		// compare the values
		// if all is good move on
		String engineId = smssProp.getProperty(ReactorKeysEnum.TOPIC_ENGINE.getKey());
		IModelEngine me = Utility.getModel(engineId);
		Map topicMap = getMap();
		
		Map inputMap = new HashMap();
		inputMap.put("topics", topicMap.keySet().toArray());
		
		List <String> inputs = getInput();
		List output = new Vector();
		
		
		for(int inputIndex = 0;inputIndex < inputs.size();inputIndex++)
		{
			Map result = (Map)me.model(inputs.get(inputIndex), insight, inputMap);
			boolean approved = isApproved(result, topicMap);
			result.put("approved", approved);
			output.add(result);
			// parse object and get threshold
			//System.err.println(result);
		}
		
		return new NounMetadata(output, PixelDataType.VECTOR);
	}
	
	private boolean isApproved(Map inputMap, Map threshold)
	{
		List labels = (List)inputMap.get("labels");
		List scores = (List)inputMap.get("scores");
		
		boolean approved = false;
		for(int labelIndex = 0;labelIndex < labels.size() && !approved;labelIndex++)
		{
			String label = (String)labels.get(labelIndex);
			double score = (Double)scores.get(labelIndex);
			double tscore = (Double)threshold.get(label);
			approved = score <= tscore;
		}
		
		return approved;
	}
	


	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	
	
	private List<String> getInput() {
		List<String> columns = new Vector<String>();

		GenRowStruct colGrs = this.store.getNoun(this.keysToGet[0]);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		} else {
			GenRowStruct inputsGRS = this.getCurRow();
			// keep track of selectors to change to upper case
			if (inputsGRS != null && !inputsGRS.isEmpty()) {
				for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
					String column = inputsGRS.get(selectIndex) + "";
					columns.add(column);
				}
			}
		}

		return columns;
	}
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[1]);
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
