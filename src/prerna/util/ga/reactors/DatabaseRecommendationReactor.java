package prerna.util.ga.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DatabaseRecommendationReactor extends AbstractRFrameReactor {
	protected static final String CLASS_NAME = DatabaseRecommendationReactor.class.getName();
	public static final String COMMUNITIES = "communities";

	public DatabaseRecommendationReactor() {
		this.keysToGet = new String[] { COMMUNITIES, ReactorKeysEnum.ACCESS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		HashMap<String, Object> recommendations = new HashMap<String, Object>();

		// check if packages are installed
		String[] packages = { "igraph", "jsonlite" };

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
			
			boolean accessFlag = getAccessBool();
			String userName = System.getProperty("user.name");
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			StringBuilder rsb = new StringBuilder();
			rsb.append(RSyntaxHelper.loadPackages(packages));
			rsb.append("source(\"" + baseFolder + "\\R\\Recommendations\\db_recom.r\"); ");;
			rsb.append( "fileroot<-\"" + baseFolder + "\\R\\Recommendations\\dataitem\" ; ");

			// run communities script
			String communityOutput = Utility.getRandomString(8);
			rsb.append(communityOutput + "<- locate_data_communities(fileroot,\"" + userName + "\");");
			rsb.append(communityOutput + "<- jsonlite::toJSON(" + communityOutput + "[3]);");
			// Step 2:
			// Run another R script to generate user specific recommendations,
			// add additional data and package as a map to be added to the list
			// for the FE.

			// run plain db recommendations script
			String userSpecificOutput = Utility.getRandomString(8);
			rsb.append(userSpecificOutput+"<- dataitem_recom_mgr(\"" + userName + "\",fileroot);");
			rsb.append(userSpecificOutput + "<- jsonlite::toJSON(as.data.table(" + userSpecificOutput + "[2])[,1:2], byrow = TRUE, colNames = TRUE);");
			this.rJavaTranslator.runR(rsb.toString().replace("\\", "/"));			
			List<String> enginesWithAccess = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());
			ArrayList<Object> communitiesList = new ArrayList<Object>();
			String communityJson = this.rJavaTranslator.getString(communityOutput);
			// the script failed or they dont have the historical data
			if (communityJson != null) {
				Gson gson = new Gson();
				ArrayList<HashMap<String, ArrayList<String>>> myList = gson.fromJson(communityJson, new TypeToken<ArrayList<HashMap<String, ArrayList<String>>>>() {}.getType());

				// parse R json response for final recommendation data
				for (int i = 0; i < myList.size(); i++) {
					HashMap<String, ArrayList<String>> map = myList.get(i);
					for (String key : map.keySet()) {
						ArrayList<String> communityMembers = map.get(key);
						ArrayList<HashMap<String, Object>> convertedMembers = new ArrayList<HashMap<String, Object>>();
						for (String member : communityMembers) {
							String[] id = member.split("\\$");
							// limit to 10 dbs. Id and name have to be included to be used
							if (id != null && id.length > 1 && communitiesList.size() < 10) {
								// only send 10 and make sure they exist on this machine
								HashMap<String, Object> engineDetail = new HashMap<String, Object>();
								String alias = id[1];
								String engId = id[0];
								boolean access = enginesWithAccess.contains(id[0]);
								if (accessFlag && !access){
									continue;
								}
								String type = "";
								if (access){
									type = (Utility.getEngine(engId)).getEngineType() + "";
								}
								engineDetail.put("appName", alias);
								engineDetail.put("appId", engId);
								engineDetail.put("appType", type);
								engineDetail.put("access", access);
								convertedMembers.add(engineDetail);
							}
						}
						if(!convertedMembers.isEmpty()){
							communitiesList.add(convertedMembers);
						}
					}
				}
			}
			recommendations.put("Communities", communitiesList);

			// parse R json response for final recommendation data
			String userSpecificJson = this.rJavaTranslator.getString(userSpecificOutput);
			// the script failed or they dont have the historical data
			ArrayList<Object> recommendationsFinal = new ArrayList<Object>();
			if (userSpecificJson != null) {
				Gson gson = new Gson();
				ArrayList<Map<String, String>> recList = gson.fromJson(userSpecificJson, new TypeToken<ArrayList<HashMap<String, String>>>() {}.getType());
				for (int i = 0; i < recList.size(); i++) {
					Map<String, String> itemMap = recList.get(i);
					if (itemMap.isEmpty() || itemMap.get("item") == null){
						continue;
					}
					String item = itemMap.get("item");
					String[] vals = item.split("\\$");
					// limit to 10 dbs. Id and name have to be included to be used
					if (vals != null && vals.length > 1 && recommendationsFinal.size() < 10) {
						// only send 10 and make sure they exist on this machine
						ArrayList<HashMap<String, Object>> convertedMembers = new ArrayList<HashMap<String, Object>>();
						HashMap<String, Object> engineDetail = new HashMap<String, Object>();
						String engId = vals[0];
						String freq = recList.get(i).get("score");
						String alias = vals[1];
						boolean access = enginesWithAccess.contains(vals[0]);
						if (accessFlag && !access){
							continue;
						}
						String type = "";
						if (access){
							type = (Utility.getEngine(engId)).getEngineType() + "";
						}
						// only return databases the user can access
						engineDetail.put("appName", alias);
						engineDetail.put("appId", engId);
						engineDetail.put("appType", type);
						engineDetail.put("freq", freq);
						engineDetail.put("access", access);
						convertedMembers.add(engineDetail);
						recommendationsFinal.add(convertedMembers);
					}
				}
			}
			recommendations.put("Recommendations", recommendationsFinal);

			// garbage cleanup -- R script might already do this
			String gc = "rm(" + communityOutput + ", " + userSpecificOutput
					+ ",blend_mgr, data_domain_mgr, read_datamatrix, exec_tfidf, "
					+ "remove_files, fileroot, blend_tracking_semantic, get_userdata, "
					+ "dataitem_history, get_dataitem_rating, assign_unique_concepts, "
					+ "populate_ratings, build_sim, cosine_jaccard_sim, cosine_sim, "
					+ "jaccard_sim, dataitem_recom_mgr, "
					+ "get_item_recom, get_user_recom, hop_away_recom_mgr, hop_away_mgr,"
					+ "locate_user_communities, drilldown_communities, locate_data_communities, "
					+ "get_items_users, refresh_base);";
			// remove packages
			this.rJavaTranslator.runR(gc + RSyntaxHelper.unloadPackages(packages));
		}
		return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.RECOMMENDATION);
	}

	private boolean getAccessBool() {
		GenRowStruct boolGrs = this.store.getNoun(ReactorKeysEnum.ACCESS.getKey());
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}
}
