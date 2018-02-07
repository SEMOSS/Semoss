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

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocumentList;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrIndexEngineQueryBuilder;

public class AppInsightsReactor extends AbstractReactor {
	
	public AppInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), "tags"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		String searchTerm = this.keyValue.get(this.keysToGet[1]);
		String limit = this.keyValue.get(this.keysToGet[2]);
		String offset = this.keyValue.get(this.keysToGet[3]);
		String modImageStr = this.keyValue.get(this.keysToGet[4]);
		List<String> tags = getTags();
		
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			builder.setSearchString(searchTerm);
			builder.setDefaultDisMaxWeighting();
		} else {
			builder.setSearchString("*:*");
		}
		
		
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
		builder.setReturnFields(retFields);

		// order the return
		builder.setSort(SolrIndexEngine.STORAGE_NAME, "asc");
		
		Integer offsetInt = null;
		Integer limitInt = null;
		if (offset != null && !offset.isEmpty()) {
			offsetInt = Integer.parseInt(offset);
			builder.setOffset(offsetInt);
		} else {
			builder.setOffset(0);
		}
		if (limit != null && !limit.isEmpty()) {
			limitInt = Integer.parseInt(limit);
			builder.setLimit(limitInt);
		} else {
			builder.setLimit(200);
		}
		
		Map<String, List<String>> filterForEngine = new HashMap<String, List<String>>();
		List<String> engineList = new ArrayList<String>();
		engineList.add(appName);
		filterForEngine.put(SolrIndexEngine.CORE_ENGINE, engineList);
		if(tags != null && !tags.isEmpty()) {
			filterForEngine.put(SolrIndexEngine.TAGS, tags);
		}
		
		builder.setFilterOptions(filterForEngine);
		
		SolrDocumentList results;
		try {
			results = SolrIndexEngine.getInstance().queryDocument(builder);
			return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INSIGHTS);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) { 
			e.printStackTrace();
			throw new IllegalArgumentException("Error retrieving insights for app = " + appName);
		}
	}

	private List<String> getTags() {
		List<String> tags = new Vector<String>();
		
		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[4]);
		if(grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for(int i = 0; i < size; i++) {
				tags.add(grs.get(i).toString());
			}
			return tags;
		}
		
		// start at index 1 and see if in cur row
		int size = this.curRow.size();
		for(int i = 4; i < size; i++) {
			tags.add(grs.get(i).toString());
		}
		return tags;
	}
	
}
