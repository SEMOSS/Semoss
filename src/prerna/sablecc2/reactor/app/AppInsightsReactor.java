package prerna.sablecc2.reactor.app;

import java.io.File;
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
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrIndexEngineQueryBuilder;
import prerna.util.DIHelper;
import prerna.util.insight.InsightScreenshot;

public class AppInsightsReactor extends AbstractReactor {
	
	public AppInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(), "includeImage", "tags"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		String search = this.keyValue.get(this.keysToGet[1]);
		String limit = this.keyValue.get(this.keysToGet[2]);
		String offset = this.keyValue.get(this.keysToGet[3]);
		String modImageStr = this.keyValue.get(this.keysToGet[4]);
		boolean modImage = (modImageStr != null && Boolean.parseBoolean(modImageStr));
		List<String> tags = getTags();
		
		SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
		builder.setDefaultDisMaxWeighting();

		if(search != null && !search.trim().isEmpty()) {
			builder.setSearchString(search);
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
		if(modImage) {
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
			// if the FE wants the images
			if(modImage) {
				String basePath = DIHelper.getInstance().getProperty("BaseFolder");
				if (results != null) {
					for (int i = 0; i < results.size(); i++) {
						SolrDocument doc = results.get(i);
						String imagePath = (String) doc.get("image");
						if (imagePath != null && imagePath.length() > 0 && !imagePath.contains("data:image/:base64")) {
							File file = new File(basePath + imagePath);
							if (file.exists()) {
								String image = InsightScreenshot.imageToString(basePath + imagePath);
								doc.put("image", "data:image/png;base64," + image);
							}
						}
					}
				}
			}
			
			return new NounMetadata(results, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INSIGHTS);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) { 
			e.printStackTrace();
			throw new IllegalArgumentException("Error retrieving results");
		}
	}

	private List<String> getTags() {
		List<String> tags = new Vector<String>();
		
		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[5]);
		if(grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for(int i = 0; i < size; i++) {
				tags.add(grs.get(i).toString());
			}
			return tags;
		}
		
		// start at index 1 and see if in cur row
		int size = this.curRow.size();
		for(int i = 5; i < size; i++) {
			tags.add(grs.get(i).toString());
		}
		return tags;
	}
	
}
