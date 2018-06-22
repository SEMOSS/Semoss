package prerna.sablecc2.reactor.masterdatabase;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrIndexEngineQueryBuilder;

public class DatabaseListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<Map<String, String>> appList = new Vector<Map<String, String>>();

		// get the list of databases from the solr app
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		builder.addReturnFields("id");
		builder.addReturnFields("app_name");
		builder.addReturnFields("app_type");
		builder.addReturnFields("app_cost");
		builder.setLimit(1000);
		builder.setSort("app_name", "asc");
		
		// account for security
		List<String> appFilters = null;
		if(this.securityEnabled()) {
			appFilters = this.getUserAppFilters();
			if(!appFilters.isEmpty()) {
				Map<String, List<String>> filterData = new HashMap<String, List<String>>();
				filterData.put("id", appFilters);
				builder.setFilterOptions(filterData);
			} else {
				return new NounMetadata(appList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
			}
		}
		
		SolrQuery q = builder.getSolrQuery();
		SolrIndexEngine solrE;
		try {
			solrE = SolrIndexEngine.getInstance();
			QueryResponse response = solrE.getQueryResponse(q, SolrIndexEngine.SOLR_PATHS.SOLR_APP_PATH_NAME);
			SolrDocumentList results = response.getResults();
			
			for(SolrDocument doc : results) {
				Map<String, String> appEntry = new HashMap<String, String>();
				// below to be removed
				appEntry.put("id", doc.get("id") + "");
				appEntry.put("name", doc.get("app_name") + "");
				// above to be removed
				appEntry.put("app_id", doc.get("id") + "");
				appEntry.put("app_name", doc.get("app_name") + "");
				appEntry.put("type", doc.get("app_type") + "");
				appEntry.put("cost", doc.get("app_cost") + "");
				appList.add(appEntry);
			}
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
			e.printStackTrace();
		}

		return new NounMetadata(appList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
	}

}
