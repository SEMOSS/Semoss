package prerna.sablecc2.reactor.app;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrIndexEngineQueryBuilder;

public class AppInsightsReactor extends AbstractReactor {
	
	public AppInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), "includeImage"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);
		String modImage = this.keyValue.get(this.keysToGet[3]);

		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		List<String> retFields = new ArrayList<String>();
		retFields.add(SolrIndexEngine.ID);
		retFields.add(SolrIndexEngine.CORE_ENGINE);
		retFields.add(SolrIndexEngine.CORE_ENGINE_ID);
		retFields.add(SolrIndexEngine.LAYOUT);
		retFields.add(SolrIndexEngine.STORAGE_NAME);
		retFields.add(SolrIndexEngine.CREATED_ON);
		retFields.add(SolrIndexEngine.MODIFIED_ON);
		retFields.add(SolrIndexEngine.LAST_VIEWED_ON);
		retFields.add(SolrIndexEngine.USER_ID);
		retFields.add(SolrIndexEngine.TAGS);
		retFields.add(SolrIndexEngine.VIEW_COUNT);
		retFields.add(SolrIndexEngine.DESCRIPTION);
		if(modImage != null && Boolean.parseBoolean(modImage)) {
			retFields.add(SolrIndexEngine.IMAGE);
		}
		builder.setReturnFields(retFields);

		// order the return
		builder.setSort(SolrIndexEngine.STORAGE_NAME, "asc");
		
		Integer offsetInt = null;
		Integer limitInt = null;
		if (offset != null && !offset.isEmpty()) {
			offsetInt = Integer.parseInt(offset);
			builder.setOffset(offsetInt);
		}
		if (limit != null && !limit.isEmpty()) {
			limitInt = Integer.parseInt(limit);
			builder.setLimit(limitInt);
		}
		
		Map<String, List<String>> filterForId = new HashMap<String, List<String>>();
		List<String> engineList = new ArrayList<String>();
		engineList.add(appName);
		filterForId.put(SolrIndexEngine.CORE_ENGINE, engineList);

		SolrDocumentList results;
		try {
			results = SolrIndexEngine.getInstance().queryDocument(builder);
			return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INSIGHTS);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) { 
			e.printStackTrace();
			throw new IllegalArgumentException("Error retrieving results");
		}
	}

}
