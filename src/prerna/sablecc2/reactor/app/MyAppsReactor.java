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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.solr.SolrIndexEngineQueryBuilder;
import prerna.util.DIHelper;
import prerna.util.insight.InsightScreenshot;

public class MyAppsReactor extends AbstractReactor {
	
	public MyAppsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Map<String, List<String>> appInfo = new HashMap<String, List<String>>();
		
		// need to get all the app names
		List<String> appNames = new Vector<String>();
		try {
			SolrIndexEngineQueryBuilder builder = new SolrIndexEngineQueryBuilder();
			builder.addReturnFields("id");
			builder.addReturnFields("app_name");

			SolrQuery q = builder.getSolrQuery();
			
			QueryResponse response = SolrIndexEngine.getInstance().getQueryResponse(q, SolrIndexEngine.SOLR_PATHS.SOLR_APP_PATH_NAME);
			// get the apps
			SolrDocumentList results = response.getResults();
			for(SolrDocument doc : results) {
				appNames.add(doc.get("app_name").toString());
			}
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
			e.printStackTrace();
		}
		
		Map<String, SolrDocumentList> appMap = null;
		try {
			// get the top images for each app
			String searchString = "*:*";
			int groupOffsetInt = 0;
			int groupLimitInt = 5;
			String groupByField = SolrIndexEngine.CORE_ENGINE;
			String groupSort = SolrIndexEngine.VIEW_COUNT;
			
			Map<String, List<String>> filters = new HashMap<String, List<String>>();
			List<String> filterList = new Vector<String>();
			filterList.add("*");
			filters.put(SolrIndexEngine.IMAGE, filterList);
			
			Map<String, Object> groupFieldMap = SolrIndexEngine.getInstance().executeQueryGroupBy(
					searchString, 
					groupOffsetInt, 
					groupLimitInt, 
					groupByField, 
					groupSort, 
					filters);
			Map<String, Object> queryRet = (Map<String, Object>) groupFieldMap.get("queryResponse");
			appMap = (Map<String, SolrDocumentList>) queryRet.get(groupByField);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) {
			e.printStackTrace();
		}
		
		// need to replace the images
		String basePath = DIHelper.getInstance().getProperty("BaseFolder");
		
		for(String appName : appNames) {
			if(appMap.containsKey(appName)) {
				List<String> images = new ArrayList<String>();
				
				SolrDocumentList list = (SolrDocumentList) appMap.get(appName);
				for (int i = 0; i < list.size(); i++) {
					SolrDocument doc = list.get(i);
					String imagePath = (String) doc.get("image");
					if (imagePath != null && !imagePath.isEmpty() && !imagePath.contains("data:image/:base64")) {
						File file = new File(basePath + imagePath);
						if (file.exists()) {
							try {
								String image = InsightScreenshot.imageToString(basePath + imagePath);
								images.add("data:image/png;base64," + image);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
				
				appInfo.put(appName, images);
			} else {
				// no images
				// just send the app
				appInfo.put(appName, new ArrayList<String>());
			}
		}

		return new NounMetadata(appInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}

}
