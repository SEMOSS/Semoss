package prerna.util.ga.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DatabaseRecommendationReactor extends AbstractRFrameReactor {
	protected static final String CLASS_NAME = DatabaseRecommendationReactor.class.getName();
	public static final String COMMUNITIES = "communities";

	public DatabaseRecommendationReactor() {
		this.keysToGet = new String[] { COMMUNITIES };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		List<Object> recommendations = new ArrayList<Object>();

		// check if packages are installed
		String[] packages = { "RGoogleAnalytics", "httr", "data.table", "jsonlite", "plyr", "igraph", "proxy" };

		String packageError = "";
		int[] confirmedPackages = this.rJavaTranslator.getIntArray("which(as.logical(lapply(list('" + StringUtils.join(packages, "','") + "')" + ", require, character.only=TRUE))==F)");
		// missing packages
		if (confirmedPackages.length > 0) {
			for (int i : confirmedPackages) {
				int index = i - 1;
				packageError += packages[index] + "\n";
			}
			String errorMessage = "\nMake sure you have all the following R libraries installed:\n" + packageError;
			logger.info(errorMessage);
		} else {
			// Step 1:
			// Run an R script to generate all communities, then get
			// additional data for each engine that exists on this machine,
			// package it up as a map to add to a list of outputs for the FE.
			
			String userName = System.getProperty("user.name");
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			String script = "source(\"" + baseFolder + "\\R\\Recommendations\\db_recom.r\"); ";
			script += "fileroot<-\"" + baseFolder + "\\R\\Recommendations\\dataitem\" ; ";
			script = script.replace("\\", "/");

			// execute source script
			this.rJavaTranslator.runR(script);

			List<String> enginesWithAccess = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			// run communities script
			script = "output<- locate_data_communities(fileroot,\"" + userName + "\");  output<-toJSON(output[3]);";
			this.rJavaTranslator.runR(script);
			ArrayList<Object> communitiesList = new ArrayList<Object>();

			String json = this.rJavaTranslator.getString("output;");
			// the script failed or they dont have the historical data
			if (json != null) {
				Gson gson = new Gson();
				ArrayList<HashMap<String, ArrayList<String>>> myList = gson.fromJson(json, new TypeToken<ArrayList<HashMap<String, ArrayList<String>>>>() {}.getType());

				// parse R json response for final recommendation data
				for (int i = 0; i < myList.size(); i++) {
					HashMap<String, ArrayList<String>> map = myList.get(i);
					for (String key : map.keySet()) {
						ArrayList<String> communityMembers = map.get(key);
						ArrayList<HashMap<String, String>> convertedMembers = new ArrayList<HashMap<String, String>>();
						for (String member : communityMembers) {
							String[] id = member.split("\\$");
							if (id != null && communitiesList.size() < 10 && enginesWithAccess.contains(id[0])) {
								// only send 10 and make sure they exist on this machine
								HashMap<String, String> engineDetail = new HashMap<String, String>();
								String alias = MasterDatabaseUtility.getEngineAliasForId(id[0]);
								String engId = id[0];
								String type = (Utility.getEngine(engId)).getEngineType() + "";
								engineDetail.put("appName", alias);
								engineDetail.put("appId", engId);
								engineDetail.put("appType", type);
								convertedMembers.add(engineDetail);
							}
						}
						communitiesList.add(convertedMembers);
					}
				}
			}
			recommendations.add(communitiesList);

			// Step 2:
			// Run another R script to generate user specific recommendations,
			// add additional data and package as a map to be added to the list
			// for the FE.

			// run plain db recommendations script
			script = "output<- dataitem_recom_mgr(\"" + userName + "\",fileroot,0.02);  output<-toJSON(as.data.table(output[2])[,1:2], byrow = TRUE, colNames = TRUE);";
			this.rJavaTranslator.runR(script);

			// parse R json response for final recommendation data
			json = this.rJavaTranslator.getString("output;");
			// the script failed or they dont have the historical data
			ArrayList<Object> recommendationsFinal = new ArrayList<Object>();
			if (json != null) {
				ArrayList<Object> recommendList = new ArrayList<Object>();
				Gson gson = new Gson();
				ArrayList<Map<String, String>> recList = gson.fromJson(json, new TypeToken<ArrayList<HashMap<String, String>>>() {}.getType());
				for (int i = 0; i < recList.size(); i++) {
					String item = recList.get(i).get("item");
					String[] vals = item.split("\\$");
					if (vals != null && recommendationsFinal.size() < 10 && enginesWithAccess.contains(vals[0])) {
						// only send 10 and make sure they exist on this machine
						ArrayList<HashMap<String, String>> convertedMembers = new ArrayList<HashMap<String, String>>();
						HashMap<String, String> engineDetail = new HashMap<String, String>();
						String engId = vals[0];
						String freq = recList.get(i).get("freq");
						String alias = MasterDatabaseUtility.getEngineAliasForId(engId);
						String type = (Utility.getEngine(engId)).getEngineType() + "";

						// only return databases the user can access
						engineDetail.put("appName", alias);
						engineDetail.put("appId", engId);
						engineDetail.put("appType", type);
						engineDetail.put("freq", freq);
						convertedMembers.add(engineDetail);
						recommendList.add(convertedMembers);
					}
				}
			}
			recommendations.add(recommendationsFinal);

			// garbage cleanup -- R script might already do this
			String gc = "rm(fileroot, output, blend_tracking_semantic, get_userdata, dataitem_history, get_dataitem_rating, assign_unique_concepts, populate_ratings, build_sim, cosine_jaccard_sim, cosine_sim, jaccard_sim, apply_tfidf, compute_weight, dataitem_recom_mgr, get_item_recom, get_user_recom, hop_away_recom_mgr, hop_away_mgr, locate_user_communities, drilldown_communities, locate_data_communities, get_items_users, refresh_base);";
			this.rJavaTranslator.runR(gc);
		}
		return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.RECOMMENDATION);
	}
}
