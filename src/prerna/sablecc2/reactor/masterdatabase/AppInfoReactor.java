package prerna.sablecc2.reactor.masterdatabase;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrIndexEngineQueryBuilder;

public class AppInfoReactor extends AbstractReactor {
	
	public AppInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		builder.addReturnFields("id");
		builder.addReturnFields("app_name");
		builder.addReturnFields("app_description");
		builder.addReturnFields("app_image");
		builder.addReturnFields("app_release_date");
		builder.addReturnFields("app_requirements");
		builder.addReturnFields("app_repo_url");
		builder.addReturnFields("app_tags");

		Map<String, List<String>> filterData = new HashMap<String, List<String>>();
		List<String> filterList = new ArrayList<String>();
		filterList.add(appName);
		filterData.put("app_name", filterList);
		builder.setFilterOptions(filterData);
		
		SolrQuery q = builder.getSolrQuery();
		SolrDocumentList results = null;
		try {
			QueryResponse response = SolrIndexEngine.getInstance().getQueryResponse(q, SolrIndexEngine.SOLR_PATHS.SOLR_APP_PATH_NAME);
			results = response.getResults();
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
			e.printStackTrace();
		}

		return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

}
