package prerna.solr.reactor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SetInsightMetadataReactor extends AbstractInsightReactor {
	
	private static final String META = "meta";
	
	public SetInsightMetadataReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), META, 
				ReactorKeysEnum.ENCODED.getKey(), ReactorKeysEnum.JSON_CLEANUP.getKey()
			};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.getProject(); 
		String insightId = this.getRdbmsId();
		
		if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, insightId)) {
			throw new IllegalArgumentException("Insight does not exist or user does not have access to edit");
		}
		
		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityInsightUtils.getAllMetakeys();
		if(!validMetakeys.containsAll(metadata.keySet())) {
	    	throw new IllegalArgumentException("Unallowed metakeys. Can only use: "+String.join(", ", validMetakeys));
		}
		
		SecurityInsightUtils.updateInsightMetadata(projectId, insightId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the insight"));
		return noun;
	}
	
	/**
	 * Get the meta map and account for encoded input or not
	 * @return
	 */
	protected Map<String, Object> getMetaMap() {
		Boolean encoded = Boolean.parseBoolean( this.keyValue.get(ReactorKeysEnum.ENCODED.getKey()) + "");
		GenRowStruct metaGrs = this.store.getNoun(META);
		if(encoded) {
			if(metaGrs != null && !metaGrs.isEmpty()) {
				List<NounMetadata> encodedStrInputs = metaGrs.getNounsOfType(PixelDataType.CONST_STRING);
				if(encodedStrInputs != null && !encodedStrInputs.isEmpty()) {
					String encodedStr = (String) encodedStrInputs.get(0).getValue();
					String decodedStr = Utility.decodeURIComponent(encodedStr);
					return new Gson().fromJson(decodedStr, Map.class);
				}
			}
	
			List<NounMetadata> encodedStrInputs = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
			if(encodedStrInputs != null && !encodedStrInputs.isEmpty()) {
				String encodedStr = (String) encodedStrInputs.get(0).getValue();
				String decodedStr = Utility.decodeURIComponent(encodedStr);
				return new Gson().fromJson(decodedStr, Map.class);
			}
		} else {
			Boolean jsonCleanup = Boolean.parseBoolean( this.keyValue.get(ReactorKeysEnum.JSON_CLEANUP.getKey()) + "");
			if(metaGrs != null && !metaGrs.isEmpty()) {
				List<NounMetadata> mapInputs = metaGrs.getNounsOfType(PixelDataType.MAP);
				if(mapInputs != null && !mapInputs.isEmpty()) {
					Map<String, Object> retMap = (Map<String, Object>) mapInputs.get(0).getValue();
					if(jsonCleanup) {
						cleanupMap(retMap);
					}
					return retMap;
				}
			}
	
			List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				Map<String, Object> retMap = (Map<String, Object>) mapInputs.get(0).getValue();
				if(jsonCleanup) {
					cleanupMap(retMap);
				}
				return retMap;
			}
		}

		throw new IllegalArgumentException("Must define a metadata map");
	}
	
	protected void cleanupMap(Map<String, Object> map) {
		Map<String, Object> replacements = new HashMap<>();
		for(String key : map.keySet()) {
			Object value = map.get(key);
			if(value instanceof Map) {
				cleanupMap((Map<String, Object>) value);
			}
			if(value instanceof Collection) {
				List<Object> newList = new ArrayList<>();
				for(Object o : (Collection) value) {
					if(o instanceof String) {
						newList.add( ((String) o).replaceAll("\\\\n", "\n").replaceAll("\\\\t", "\t") );
					} else {
						newList.add(o);
					}
				}
				replacements.put(key, value);
			} else if(value instanceof String) {
				value = ((String) value).replaceAll("\\\\n", "\n").replaceAll("\\\\t", "\t");
				replacements.put(key, value);
			}
		}
		
		map.putAll(replacements);
	}
	
	@Override
	public String getReactorDescription() {
		return "Define metadata for an insight";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the insight. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}
}
