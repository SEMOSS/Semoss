package prerna.engine.impl.model.reactors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONObject;

import prerna.reactor.AbstractReactor;
import prerna.reactor.utils.GetRequestReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetMyOpenAiKeyModelsListReactor extends AbstractReactor {
	
	private static final String OPENAI_MODEL_LIST_ENDPOINT = "https://api.openai.com/v1/models";
	
	public GetMyOpenAiKeyModelsListReactor() {
		this.keysToGet = new String[]{"openaiKey" , "organizationId"};
		this.keyRequired = new int [] { 1 , 0 };
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String openaiKey = this.keyValue.get(this.keysToGet[0]);
		String organizationId = this.keyValue.get(this.keysToGet[1]);
		
		NounStore ns = new NounStore(ReactorKeysEnum.ALL.getKey());
		ns.makeNoun(ReactorKeysEnum.URL.getKey()).addLiteral(OPENAI_MODEL_LIST_ENDPOINT);
		
		Map<Object, Object> headersMap = new HashMap<>();
		headersMap.put("Authorization", "Bearer " + openaiKey);
		if (organizationId != null && !organizationId.isEmpty()) {
			headersMap.put("OpenAI-Organization", organizationId);
		}
		ns.makeNoun("headersMap").addMap(headersMap);
		GetRequestReactor getRequest = new GetRequestReactor();	
		getRequest.setInsight(this.insight);
		getRequest.setNounStore(ns);
		getRequest.In();
		NounMetadata requestResponse = getRequest.execute();
		if (requestResponse.getNounType().equals(PixelDataType.ERROR)) { // server side errors
            throw new IllegalArgumentException("ERROR: " + requestResponse.getExplanation());
        }
		
		String responseString = (String) requestResponse.getValue();
		JSONObject response = new JSONObject(responseString);
		JSONArray modelDetails = response.getJSONArray("data");
		HashSet<String> modelList = new HashSet<>();
		for (int i = 0 ; i < modelDetails.length(); i++) {
			JSONObject modleInfo = modelDetails.getJSONObject(i);
			modelList.add(modleInfo.getString("root"));
		}
		
		return new NounMetadata(new TreeSet<>(modelList), PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("openaiKey")) {
			return "Open AI Key obtained from https://platform.openai.com/account/api-keys";
		} else if(key.equals("organizationId")) {
			return "(Optional) For users who belong to multiple organizations, you can pass a header to specify which organization is used for an API request";
		}
	
		return super.getDescriptionForKey(key);
	}
}