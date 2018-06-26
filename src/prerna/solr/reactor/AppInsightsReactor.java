package prerna.solr.reactor;

import java.util.List;
import java.util.Map;

import prerna.auth.SecurityQueryUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class AppInsightsReactor extends AbstractReactor {
	
	public AppInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), 
				ReactorKeysEnum.FILTER_WORD.getKey(), 
				ReactorKeysEnum.LIMIT.getKey(), 
				ReactorKeysEnum.OFFSET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String searchTerm = this.keyValue.get(this.keysToGet[1]);
		String limit = this.keyValue.get(this.keysToGet[2]);
		String offset = this.keyValue.get(this.keysToGet[3]);
		
		List<Map<String, String>> results = null;
		if(this.securityEnabled()) {
			if(this.getUserAppFilters().contains(appId)) {
				results = SecurityQueryUtils.searchUserInsights(appId, this.insight.getUserId(), searchTerm, limit, offset);
			} else {
				throw new IllegalArgumentException("App does not exist or user does not have access to database");
			}
		} else {
			results = SecurityQueryUtils.searchInsights(appId, searchTerm, limit, offset);
		}
		
		return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INSIGHTS);

//		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
//		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
//			builder.setSearchString(searchTerm);
//			if(!searchTerm.equals("*:*")) {
//				builder.setDefaultDisMaxWeighting();
//			}
//		} else {
//			builder.setSearchString("*:*");
//		}
//		
//		List<String> retFields = new ArrayList<String>();
//		retFields.add(SolrIndexEngine.ID);
//		retFields.add(SolrIndexEngine.APP_ID);
//		retFields.add(SolrIndexEngine.APP_NAME);
//		retFields.add(SolrIndexEngine.APP_INSIGHT_ID);
//		retFields.add(SolrIndexEngine.LAYOUT);
//		retFields.add(SolrIndexEngine.STORAGE_NAME);
//		retFields.add(SolrIndexEngine.CREATED_ON);
//		retFields.add(SolrIndexEngine.MODIFIED_ON);
//		retFields.add(SolrIndexEngine.LAST_VIEWED_ON);
//		retFields.add(SolrIndexEngine.USER_ID);
//		retFields.add(SolrIndexEngine.TAGS);
//		retFields.add(SolrIndexEngine.VIEW_COUNT);
//		retFields.add(SolrIndexEngine.DESCRIPTION);
//		builder.setReturnFields(retFields);
//
//		// order the return
//		builder.setSort(SolrIndexEngine.STORAGE_NAME, "asc");
//		
//		Integer offsetInt = null;
//		Integer limitInt = null;
//		if (offset != null && !offset.isEmpty()) {
//			offsetInt = Integer.parseInt(offset);
//			builder.setOffset(offsetInt);
//		} else {
//			builder.setOffset(0);
//		}
//		if (limit != null && !limit.isEmpty()) {
//			limitInt = Integer.parseInt(limit);
//			builder.setLimit(limitInt);
//		} else {
//			builder.setLimit(200);
//		}
//		
//		Map<String, List<String>> filterForEngine = new HashMap<String, List<String>>();
//		List<String> engineIdList = new ArrayList<String>();
//		engineIdList.add(appId);
//		filterForEngine.put(SolrIndexEngine.APP_ID, engineIdList);
//		if(tags != null && !tags.isEmpty()) {
//			filterForEngine.put(SolrIndexEngine.TAGS, tags);
//		}
//		
//		builder.setFilterOptions(filterForEngine);
//		
//		SolrDocumentList results;
//		try {
//			results = SolrIndexEngine.getInstance().queryDocument(builder);
//			return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INSIGHTS);
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) { 
//			e.printStackTrace();
//			throw new IllegalArgumentException("Error retrieving insights for app = " + appId);
//		}
	}
	
}
