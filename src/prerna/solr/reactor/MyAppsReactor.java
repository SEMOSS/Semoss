package prerna.solr.reactor;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrIndexEngineQueryBuilder;

public class MyAppsReactor extends AbstractReactor {
	
	public MyAppsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		List<Map<String, Object>> appInfo = null;
		Map<String, Map<String, Long>> facetInfo = null;
		
		// account for security
		List<String> appFilters = null;
		if(this.securityEnabled()) {
			appFilters = this.getUserAppFilters();
			if(!appFilters.isEmpty()) {
				appInfo = getAppInfo(appFilters);
				facetInfo = getFacetInfo(appFilters);
			} else {
				appInfo = new Vector<Map<String, Object>>();
				facetInfo = new HashMap<String, Map<String, Long>>();
			}
		} else {
			appInfo = getAppInfo(appFilters);
			facetInfo = getFacetInfo(appFilters);
		}
		
		Map<String, Object> myApps = new HashMap<String, Object>();
		myApps.put("appInfo", appInfo);
		myApps.put("appFacets", facetInfo);
		return new NounMetadata(myApps, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INFO);
	}

	
	/**
	 * Get the info about the apps
	 * This will return the following information
	 * [{
	 * 	"app_name" : "name" // stored within app core
	 * 	"app_description" : "description" // stored within app core
	 * 	"app_tags" : ["tag1","tag2",...] // stored within app core
	 * 	"insights : ["image1", .. "image5] // stored within insight core
	 * },
	 * {
	 * ...
	 * },
	 * ...
	 * }]
	 * @return
	 */
	private List<Map<String, Object>> getAppInfo(List<String> appFilters) {
		String searchTerm = this.keyValue.get(this.keysToGet[0]);
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);

		List<Map<String, Object>> appInfo = new Vector<Map<String, Object>>();
		
		try {
			SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
			if(searchTerm != null && !searchTerm.trim().isEmpty()) {
				builder.setSearchString(searchTerm);
				builder.setDefaultDisMaxWeighting();
			} else {
				builder.setSearchString("*:*");
			}
			builder.addReturnFields("id");
			builder.addReturnFields("app_name");
			builder.addReturnFields("app_description");
			builder.addReturnFields("app_tags");
			builder.addReturnFields("app_type");
			builder.addReturnFields("app_cost");
			builder.setSort("app_name", "asc");
			if(limit == null) {
				builder.setLimit(100);
			} else {
				builder.setLimit(Integer.parseInt(limit));
			}
			if(offset == null) {
				builder.setOffset(0);
			} else {
				builder.setLimit(Integer.parseInt(offset));
			}
			
			if(appFilters != null) {
				Map<String, List<String>> filterData = new HashMap<String, List<String>>();
				filterData.put("id", appFilters);
				builder.setFilterOptions(filterData);
			}
			
			SolrQuery q = builder.getSolrQuery();
			
			QueryResponse response = SolrIndexEngine.getInstance().getQueryResponse(q, SolrIndexEngine.SOLR_PATHS.SOLR_APP_PATH_NAME);
			// get the apps
			SolrDocumentList results = response.getResults();
			for(SolrDocument doc : results) {
				Map<String, Object> appMap = new HashMap<String, Object>();
				appMap.put("app_id", doc.get("id"));
				appMap.put("app_name", doc.get("app_name"));
				appMap.put("app_description", doc.get("app_description"));
				appMap.put("app_tags", doc.get("app_tags"));
				appMap.put("app_type", doc.get("app_type"));
				appMap.put("app_cost", doc.get("app_cost"));
				appInfo.add(appMap);
			}
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
			e.printStackTrace();
		}
		
		Map<String, SolrDocumentList> imageMap = null;
		try {
			// get the top images for each app
			String searchString = "*:*";
			int groupOffsetInt = 0;
			int groupLimitInt = 25;
			String groupByField = SolrIndexEngine.APP_ID;
			String groupSort = SolrIndexEngine.VIEW_COUNT;
			
			Map<String, Object> groupFieldMap = SolrIndexEngine.getInstance().executeQueryGroupBy(
					searchString, 
					groupOffsetInt, 
					groupLimitInt, 
					groupByField, 
					groupSort, 
					new HashMap<String, List<String>>());
			Map<String, Object> queryRet = (Map<String, Object>) groupFieldMap.get("queryResponse");
			imageMap = (Map<String, SolrDocumentList>) queryRet.get(groupByField);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) {
			e.printStackTrace();
		}
		
		for(Map<String, Object> appMap : appInfo) {
			String appName = appMap.get("app_name") + "";
			// do we have images that we can use to make 
			// the information look pretty
			if(imageMap.containsKey(appName)) {
				List<String> insights = new Vector<String>();
				List<String> names = new Vector<String>();

				SolrDocumentList list = (SolrDocumentList) imageMap.get(appName);
				for (int i = 0; i < list.size(); i++) {
					SolrDocument doc = list.get(i);
					insights.add((String) doc.get(SolrIndexEngine.APP_INSIGHT_ID));
					names.add((String) doc.get(SolrIndexEngine.STORAGE_NAME));
				}
				
				appMap.put("rdbmsId", insights);
				appMap.put("insightName", names);
			}
		}
		
		return appInfo;
	}
	
	/**
	 * Get the facet information
	 * This will return the following information
	 * {
	 * 	"facetField1" : {
	 * 					"value1" : numberOfTimesItAppears,
	 * 					"value2" : numberOfTimesItAppears,
	 * 					}
	 * 
	 * 	"facetField2" : {
	 * 					"value1" : numberOfTimesItAppears,
	 * 					"value2" : numberOfTimesItAppears,
	 * 					}
	 * 	...
	 * }
	 * @return
	 */
	private Map<String, Map<String, Long>> getFacetInfo(List<String> appFilters) {
		String searchTerm = this.keyValue.get(this.keysToGet[0]);
		
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			builder.setSearchString(searchTerm);
			builder.setDefaultDisMaxWeighting();
		} else {
			builder.setSearchString("*:*");
		}
		
		List<String> facetList = new ArrayList<>();
		facetList.add("app_tags");
		// set facet info
		builder.setFacet(true);
		builder.setFacetField(facetList);
		builder.setFacetMinCount(1);
		builder.setFacetSortCount(true);
		
		if(appFilters != null) {
			Map<String, List<String>> filterData = new HashMap<String, List<String>>();
			filterData.put("id", appFilters);
			builder.setFilterOptions(filterData);
		}
		
		Map<String, Map<String, Long>> facetInfo = null;
		
		try {
			SolrIndexEngine solrE = SolrIndexEngine.getInstance();
			QueryResponse response = solrE.getQueryResponse(builder.getSolrQuery(), SolrIndexEngine.SOLR_PATHS.SOLR_APP_PATH_NAME);
			List<FacetField> facetFieldList = response.getFacetFields();
			facetInfo = solrE.processFacetFieldMap(facetFieldList);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
			e.printStackTrace();
		}
		
		return facetInfo;
	}
	

}
