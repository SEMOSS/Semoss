package prerna.solr.reactor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetInsightMetakeyOptionsReactor extends AbstractInsightReactor {
	
	private static final String METAOPTIONS = "metaoptions";
	
	public SetInsightMetakeyOptionsReactor() {
		this.keysToGet = new String[]{METAOPTIONS};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		boolean res = false;
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User is not an admin.");
		} else {
			organizeKeys();
			List<Map<String, Object>> metaoptions = getMetaOptions();
			if (metaoptions==null || metaoptions.isEmpty()) {
				throw new IllegalArgumentException("Must provide a set of metadata values to store.");
			}
			res = SecurityInsightUtils.updateMetakeyOptions(metaoptions);
		}
		NounMetadata noun = new NounMetadata(res, PixelDataType.BOOLEAN);
		if (res) {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully updated the new metakey for metakey options"));
		} else {
			noun.addAdditionalReturn(NounMetadata.getErrorNounMessage("Did not update metakey options. Please check your inputs and try again."));
		}
		return noun;
	}
	
	private List<Map<String,Object>> getMetaOptions() {
		GenRowStruct mapGrs = this.store.getNoun(METAOPTIONS);
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				List<Map<String,Object>> res = new ArrayList<>();
				for (int i=0; i<mapInputs.size(); i++) {
					res.add((Map<String, Object>) mapInputs.get(i).getValue());
				}
				return res;
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			List<Map<String,Object>> res = new ArrayList<>();
			for (int i=0; i<mapInputs.size(); i++) {
				res.add((Map<String, Object>) mapInputs.get(i).getValue());
			}
			return res;
		}
		return null;
	}
}
