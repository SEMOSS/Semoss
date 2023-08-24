package prerna.solr.reactor;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetAvailableTagsReactor extends AbstractReactor {

	public GetAvailableTagsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		List<String> inputFilters = getProjectFilters();
		List<NounMetadata> warningNouns = new Vector<>();
		List<String> appliedProjectFilters = null;

		// account for security
		List<String> projectFilters = null;
		appliedProjectFilters = new Vector<>();
		projectFilters = SecurityProjectUtils.getFullUserProjectIds(this.insight.getUser());
		if(!inputFilters.isEmpty()) {
			// loop through and compare what the user has access to
			for(String inputAppFilter : inputFilters) {
				if(!projectFilters.contains(inputAppFilter)) {
					warningNouns.add(NounMetadata.getWarningNounMessage(inputAppFilter + " does not exist or user does not have access to project."));
				} else {
					appliedProjectFilters.add(inputAppFilter);
				}
			}
		} else {
			// set the permissions to everything the user has access to
			appliedProjectFilters.addAll(projectFilters);
		}
//		else {
//			// no security
//			// keep null, we will not have an database filter
//		}
		
		if(appliedProjectFilters != null && appliedProjectFilters.isEmpty()) {
			if(inputFilters.isEmpty()) {
				return NounMetadata.getWarningNounMessage("User does not have access to any projects");
			} else {
				return NounMetadata.getErrorNounMessage("Input project filters do not exist or user does not have access to the projects");
			}
		}
		
		List<Map<String, Object>> ret = SecurityInsightUtils.getAvailableInsightTagsAndCounts(appliedProjectFilters);
		NounMetadata noun = new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
		if(!warningNouns.isEmpty()) {
			noun.addAllAdditionalReturn(warningNouns);
		}
		
		return noun;
	}
	
	private List<String> getProjectFilters() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return this.curRow.getAllStrValues();
	}
	
}
