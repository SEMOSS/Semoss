package prerna.solr.reactor;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightsReactor extends AbstractReactor {
	
	public GetInsightsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		GenRowStruct engineFilterGrs = this.store.getNoun(this.keysToGet[0]);
		List<NounMetadata> warningNouns = new Vector<>();
		// get list of engineIds if user has access
		List<String> eFilters = null;
		if (engineFilterGrs != null && !engineFilterGrs.isEmpty()) {
			eFilters = new Vector<String>();
			for (int i = 0; i < engineFilterGrs.size(); i++) {
				String engineFilter = engineFilterGrs.get(i).toString();
				if (AbstractSecurityUtils.securityEnabled()) {
					engineFilter = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineFilter);
					if (SecurityAppUtils.userCanViewEngine(this.insight.getUser(), engineFilter)) {
						eFilters.add(engineFilter);
					} else {
						// store warnings
						warningNouns.add(NounMetadata.getWarningNounMessage(engineFilter + " does not exist or user does not have access to database."));
					}
				} else {
					engineFilter = MasterDatabaseUtility.testEngineIdIfAlias(engineFilter);
					eFilters.add(engineFilter);
				}
			}
		}
		String searchTerm = this.keyValue.get(this.keysToGet[1]);
		String limit = this.keyValue.get(this.keysToGet[2]);
		String offset = this.keyValue.get(this.keysToGet[3]);
		// get results
		List<Map<String, Object>> results = null;
		// method handles if filters are null or not
		if (AbstractSecurityUtils.securityEnabled()) {
			results = SecurityInsightUtils.searchUserInsights(this.insight.getUser(), eFilters, searchTerm, limit,offset);
		} else {
			results = SecurityInsightUtils.searchInsights(eFilters, searchTerm, limit, offset);
		}
		
		NounMetadata retNoun = new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INSIGHTS);
		// add warnings to results
		if(!warningNouns.isEmpty()) {
			for(NounMetadata warning: warningNouns) {
				retNoun.addAdditionalReturn(warning);
			}
		}
		return retNoun;
	}
	
}
