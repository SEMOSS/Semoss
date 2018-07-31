package prerna.util.ga.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DatabaseRecommendationReactor extends AbstractRFrameReactor  {
	
	public static final String COMMUNITIES = "communities";
	
	public DatabaseRecommendationReactor() {
		this.keysToGet = new String[] {COMMUNITIES};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();

		// check if packages are installed
		String[] packages = { "RGoogleAnalytics", "httr", "data.table", "jsonlite", "plyr", "igraph", "proxy" };
		this.rJavaTranslator.checkPackages(packages);
		boolean communities = getBool();
		
		// get user name 
		String userName = System.getProperty("user.name");

		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String script = "source(\"" + baseFolder + "\\R\\Recommendations\\db_recom.r\"); ";
		script += "fileroot<-\"" + baseFolder + "\\R\\Recommendations\\dataitem\" ; ";
		script = script.replace("\\", "/");

		// execute source script
		this.rJavaTranslator.runR(script);
		
		List<Object> recommendations = new ArrayList<Object>();
		List<String> enginesWithAccess = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
		if (communities) {
			// run communities script
			script = "output<- locate_data_communities(fileroot,\"" + userName + "\");  output<-toJSON(output[3]);";
			this.rJavaTranslator.runR(script);

			String json = this.rJavaTranslator.getString("output;");
			// the script failed or they dont have the historical data
			if(json == null){
				return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.RECOMMENDATION);
			}
			Gson gson = new Gson();
			ArrayList<HashMap<String, ArrayList<String>>> myList = gson.fromJson(json, new TypeToken<ArrayList<HashMap<String, ArrayList<String>>>>(){}.getType());

			// parse R json response for final recommendation data
			for (int i = 0; i < myList.size(); i++) {
				HashMap<String, ArrayList<String>> map = myList.get(i);
				for (String key : map.keySet()) {
					ArrayList<String> communityMembers = map.get(key);
					ArrayList<HashMap<String, String>> convertedMembers = new ArrayList<HashMap<String, String>>();
					for (String member : communityMembers) {
						HashMap<String, String> engineDetail = new HashMap<String, String>();
						String[] id = member.split("\\$");
						// only show engines on my machine that I have access to
						if (enginesWithAccess.contains(id[0])) {
							String alias = MasterDatabaseUtility.getEngineAliasForId(id[0]);
							String engId = id[0];
							String type = (Utility.getEngine(engId)).getEngineType() + "";
							engineDetail.put("appName", alias);
							engineDetail.put("appId", engId);
							engineDetail.put("appType", type);
							convertedMembers.add(engineDetail);
						}
					}
					if (convertedMembers != null && !convertedMembers.isEmpty()){
						recommendations.add(convertedMembers);
					}
				}
			}
		} else {
			// run plain db recommendations script
			script = "output<- dataitem_recom_mgr(\"" + userName + "\",fileroot,0.02);  output<-toJSON(as.data.table(output[2])[,1:2], byrow = TRUE, colNames = TRUE);";
			this.rJavaTranslator.runR(script);

			// parse R json response for final recommendation data
			String json = this.rJavaTranslator.getString("output;");
			// the script failed or they dont have the historical data
			if(json == null){
				return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.RECOMMENDATION);
			}
			Gson gson = new Gson();
			ArrayList<Map<String, String>> myList = gson.fromJson(json,	new TypeToken<ArrayList<HashMap<String, String>>>() {}.getType());
			for (int i = 0; i < myList.size(); i++) {
				Map<String, String> entry = new HashMap<String, String>();
				String item = myList.get(i).get("item");
				String[] vals = item.split("\\$");
				if (vals == null) {
					continue;
				}
				// only return databases the user can access
				if (enginesWithAccess.contains(vals[0])) {
					entry.put(MasterDatabaseUtility.getEngineAliasForId(vals[0]), vals[0]);
					recommendations.add(entry);
				}
			}
		}
		
		// garbage cleanup -- R script might already do this
		String gc = "rm(fileroot, output, blend_tracking_semantic, get_userdata, dataitem_history, get_dataitem_rating, assign_unique_concepts, populate_ratings, build_sim, cosine_jaccard_sim, cosine_sim, jaccard_sim, apply_tfidf, compute_weight, dataitem_recom_mgr, get_item_recom, get_user_recom, hop_away_recom_mgr, hop_away_mgr, locate_user_communities, drilldown_communities, locate_data_communities, get_items_users, refresh_base);";
		this.rJavaTranslator.runR(gc);
		
		return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.RECOMMENDATION);
	}

	private boolean getBool() {
		GenRowStruct boolGrs = this.store.getNoun(COMMUNITIES);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return true;
	}

}
