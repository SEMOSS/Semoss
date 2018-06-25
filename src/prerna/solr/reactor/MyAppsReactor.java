package prerna.solr.reactor;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import prerna.auth.SecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;

public class MyAppsReactor extends AbstractReactor {

	public MyAppsReactor() {

	}

	@Override
	public NounMetadata execute() {
		List<Map> appInfo = new Vector<Map>();

		if(this.securityEnabled()) {
			appInfo.addAll(SecurityUtils.getUserDatabaseList(this.insight.getUserId()));
		} else {
			appInfo.addAll(SecurityUtils.getAllDatabaseList());
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

		for(Map appMap : appInfo) {
			String appName = appMap.get("app_id") + "";
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
		
		return new NounMetadata(appInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.APP_INFO);
	}

}
