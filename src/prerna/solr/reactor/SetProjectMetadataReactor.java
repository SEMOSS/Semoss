package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;

public class SetProjectMetadataReactor extends AbstractReactor {
	
	private static final String META = "meta";
	
	public SetProjectMetadataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), META};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
			if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
				throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
			}
		}
		
		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityProjectUtils.getAllMetakeys();
		if(!validMetakeys.containsAll(metadata.keySet())) {
	    	throw new IllegalArgumentException("Unallowed metakeys. Can only use: "+String.join(", ", validMetakeys));
		}
		
		SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the project"));
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
		return "Define metadata on a project";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the database. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}

}
