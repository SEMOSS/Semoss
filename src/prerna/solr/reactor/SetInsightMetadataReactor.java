package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;

public class SetInsightMetadataReactor extends AbstractInsightReactor {
	
	private static final String META = "meta";
	
	public SetInsightMetadataReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), META
			};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.getProject(); 
		String insightId = this.getRdbmsId();
		
		if(AbstractSecurityUtils.securityEnabled()) {
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, insightId)) {
				throw new IllegalArgumentException("Insight does not exist or user does not have access to edit");
			}
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
	 * 
	 * @return
	 */
	private Map<String, Object> getMetaMap() {
		GenRowStruct mapGrs = this.store.getNoun(META);
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

		throw new IllegalArgumentException("Must define a metadata map");
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
