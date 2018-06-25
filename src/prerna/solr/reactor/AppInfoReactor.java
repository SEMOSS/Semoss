package prerna.solr.reactor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngineQueryBuilder;

public class AppInfoReactor extends AbstractReactor {
	
	public AppInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		builder.addReturnFields("id");
		builder.addReturnFields("app_name");
		builder.addReturnFields("app_description");
		builder.addReturnFields("app_image");
		builder.addReturnFields("app_creation_date");
		builder.addReturnFields("app_requirements");
		builder.addReturnFields("app_repo_url");
		builder.addReturnFields("app_tags");
		builder.addReturnFields("app_type");
		builder.addReturnFields("app_cost");

		Map<String, List<String>> filterData = new HashMap<String, List<String>>();
		List<String> filterList = new ArrayList<String>();
		filterList.add(appId);
		filterData.put("id", filterList);
		builder.setFilterOptions(filterData);
		
		SolrQuery q = builder.getSolrQuery();
		SolrDocument appInfo = null;
//		try {
//			SolrIndexEngine solrE = SolrIndexEngine.getInstance();
//			QueryResponse response = solrE.getQueryResponse(q, SolrIndexEngine.SOLR_PATHS.SOLR_APP_PATH_NAME);
//			SolrDocumentList results = response.getResults();
//			// there should only be 1
//			appInfo = results.get(0);
//			appInfo.put("app_id", appInfo.remove("id"));
//			
//			/*
//			 * Queries below are done on the insight core
//			 */
//			
//			// augment the metadata with the number of insights in the app
//			long numInsights = solrE.getNumEngineInsights(appId);
//			appInfo.put("num_insights", numInsights);
//			
//			// augment the metadata with the tags for the insights in the app
//			builder = new SolrIndexEngineQueryBuilder();
//			// need to re-add the filter but modify the key name
//			filterData = new HashMap<String, List<String>>();
//			filterList = new ArrayList<String>();
//			filterList.add(appId);
//			filterData.put(SolrIndexEngine.APP_ID, filterList);
//			builder.setFilterOptions(filterData);
//			
//			List<String> facetList = new ArrayList<>();
//			facetList.add(SolrIndexEngine.TAGS);
//			// set facet info
//			builder.setFacet(true);
//			builder.setFacetField(facetList);
//			builder.setFacetMinCount(1);
//			builder.setFacetSortCount(true);
//			
//			response = solrE.getQueryResponse(builder.getSolrQuery(), SolrIndexEngine.SOLR_PATHS.SOLR_INSIGHTS_PATH);
//			List<FacetField> facetFieldList = response.getFacetFields();
//			Map<String, Map<String, Long>> facetInfo = solrE.processFacetFieldMap(facetFieldList);
//			appInfo.put("insight_facets", facetInfo);
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
//			e.printStackTrace();
//		}

		return new NounMetadata(appInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INFO);
	}

}
