//package prerna.sablecc2.reactor.masterdatabase;
//
//import java.io.File;
//import java.io.IOException;
//import java.security.KeyManagementException;
//import java.security.KeyStoreException;
//import java.security.NoSuchAlgorithmException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.common.SolrDocument;
//import org.apache.solr.common.SolrDocumentList;
//
//import prerna.sablecc2.om.NounMetadata;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.PixelOperationType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.reactor.AbstractReactor;
//import prerna.solr.SolrIndexEngine;
//import prerna.util.DIHelper;
//import prerna.util.insight.InsightScreenshot;
//
//public class DatabaseInfoReactor extends AbstractReactor {
//	
//	public DatabaseInfoReactor() {
//		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
//	}
//
//	@Override
//	public NounMetadata execute() {
//		String searchString = "*:*";
//		int groupOffsetInt = 0;
//		int groupLimitInt = 5;
//		String groupByField = "core_engine";
//		String groupSort = "view_count";
//		
//		Map<String, SolrDocumentList> engineMap = null;
//		try {
//			Map<String, Object> groupFieldMap = SolrIndexEngine.getInstance().executeQueryGroupBy(
//					searchString, 
//					groupOffsetInt, 
//					groupLimitInt, 
//					groupByField, 
//					groupSort, 
//					new HashMap<String, List<String>>());
//			Map<String, Object> queryRet = (Map<String, Object>) groupFieldMap.get("queryResponse");
//			engineMap = (Map<String, SolrDocumentList>) queryRet.get("core_engine");
//
//			// need to replace the images
//			String basePath = DIHelper.getInstance().getProperty("BaseFolder");
//
//			for(String enginename : engineMap.keySet()) {
//				SolrDocumentList list = (SolrDocumentList) engineMap.get(enginename);
//				for (int i = 0; i < list.size(); i++) {
//					SolrDocument doc = list.get(i);
//					String imagePath = (String) doc.get("image");
//					if (imagePath != null && !imagePath.isEmpty() && !imagePath.contains("data:image/:base64")) {
//						File file = new File(basePath + imagePath);
//						if (file.exists()) {
//							String image = InsightScreenshot.imageToString(basePath + imagePath);
//							doc.put("image", "data:image/png;base64," + image);
//						}
//					}
//				}
//			}
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) {
//			e.printStackTrace();
//		}
//
//		return new NounMetadata(engineMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
//	}
//
//}
