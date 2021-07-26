package prerna.solr.reactor;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetAvailableTagsReactor extends AbstractReactor {

	public GetAvailableTagsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		List<String> inputFilters = getAppFilters();
		List<NounMetadata> warningNouns = new Vector<>();
		List<String> appliedDatabaseFilters = null;

		// account for security
		List<String> databaseFilters = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			appliedDatabaseFilters = new Vector<>();
			databaseFilters = SecurityQueryUtils.getFullUserDatabaseIds(this.insight.getUser());
			if(!inputFilters.isEmpty()) {
				// loop through and compare what the user has access to
				for(String inputAppFilter : inputFilters) {
					if(!databaseFilters.contains(inputAppFilter)) {
						warningNouns.add(NounMetadata.getWarningNounMessage(inputAppFilter + " does not exist or user does not have access to database."));
					} else {
						appliedDatabaseFilters.add(inputAppFilter);
					}
				}
			} else {
				// set the permissions to everything the user has access to
				appliedDatabaseFilters.addAll(databaseFilters);
			}
		}
//		else {
//			// no security
//			// keep null, we will not have an database filter
//		}
		
		if(AbstractSecurityUtils.securityEnabled() && appliedDatabaseFilters != null && appliedDatabaseFilters.isEmpty()) {
			if(inputFilters.isEmpty()) {
				return NounMetadata.getWarningNounMessage("User does not have access to any databases");
			} else {
				return NounMetadata.getErrorNounMessage("Input database filters do not exist or user does not have access to the databases");
			}
		}
		
		List<Map<String, Object>> ret = SecurityInsightUtils.getAvailableInsightTagsAndCounts(appliedDatabaseFilters);
		NounMetadata noun = new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
		if(!warningNouns.isEmpty()) {
			noun.addAllAdditionalReturn(warningNouns);
		}
		
		return noun;
	}
	
	private List<String> getAppFilters() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return this.curRow.getAllStrValues();
	}
	
}
