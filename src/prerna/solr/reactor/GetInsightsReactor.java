package prerna.solr.reactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.sql.AbstractSqlQueryUtil;

public class GetInsightsReactor extends AbstractReactor {
	
	private static List<String> META_KEYS_LIST = new Vector<String>();
	static {
		META_KEYS_LIST.add("description");
		META_KEYS_LIST.add("tag");
	}
	
	public GetInsightsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(),
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), ReactorKeysEnum.TAGS.getKey() };
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
		List<String> tagFilters = getTags();
		
		// get results
		List<Map<String, Object>> results = null;
		// method handles if filters are null or not
		if (AbstractSecurityUtils.securityEnabled()) {
			results = SecurityInsightUtils.searchUserInsights(this.insight.getUser(), eFilters, searchTerm, tagFilters, limit, offset);
		} else {
			results = SecurityInsightUtils.searchInsights(eFilters, searchTerm, tagFilters, limit, offset);
		}
		
		// this entire block is to add the additional metadata to the insights
		{
			// now i will aggregate each app id to its insight ids
			// and then i will query to get all the tags + descriptions
			int size = results.size();
			Map<String, List<String>> appIdsToInsight = new HashMap<String, List<String>>();
			Map<String, Integer> index = new HashMap<String, Integer>(size);
			for(int i = 0; i < size; i++) {
				Map<String, Object> res = results.get(i);
				String appId = (String) res.get("app_id");
				String insightId = (String) res.get("app_insight_id");
				
				// aggregate + store
				List<String> inIds = null;
				if(appIdsToInsight.containsKey(appId)) {
					inIds = appIdsToInsight.get(appId);
				} else {
					inIds = new Vector<String>();
					appIdsToInsight.put(appId, inIds);
				}
				
				inIds.add(insightId);
				
				// store so we can search by index
				index.put(appId + insightId, new Integer(i));
			}
			
			for(String appId : appIdsToInsight.keySet()) {
				IRawSelectWrapper wrapper = SecurityInsightUtils.getInsightMetadataWrapper(appId, appIdsToInsight.get(appId), META_KEYS_LIST);
				while(wrapper.hasNext()) {
					Object[] data = wrapper.next().getValues();
					String metaKey = (String) data[2];
					String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) data[3]);
					if(value == null) {
						continue;
					}
					
					String unique = data[0] + "" + data[1];
					int indextofind = index.get(unique);
					
					Map<String, Object> res = results.get(indextofind);
					// right now only handling description + tags
					if(metaKey.equals("description")) {
						// we only have 1 description per insight
						// so just push
						res.put("description", value);
					} else {
						// multiple tags per insight
						List<String> tags = null;
						if(res.containsKey("tags")) {
							tags = (List<String>) res.get("tags");
						} else {
							tags = new Vector<String>();
							res.put("tags", tags);
						}
						
						// add to the list
						tags.add(value);
					}
				}
			}
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
	
	/**
	 * Get the tags to set for the insight
	 * @return
	 */
	private List<String> getTags() {
		List<String> tags = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.TAGS.getKey());
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				tags.add(grs.get(i).toString());
			}
		}
		
		return tags;
	}
}
