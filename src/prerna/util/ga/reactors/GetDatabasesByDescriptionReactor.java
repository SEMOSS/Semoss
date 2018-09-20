package prerna.util.ga.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

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

public class GetDatabasesByDescriptionReactor extends AbstractRFrameReactor  {

	protected static final String CLASS_NAME = GetDatabasesByDescriptionReactor.class.getName();

	public GetDatabasesByDescriptionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.ACCESS.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		// init and organize keys
		init();
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		ArrayList<Object> recommendations = new ArrayList<Object>();
		String workDir = "wd"+Utility.getRandomString(8);
		StringBuilder sb = new StringBuilder();
		sb.append(workDir + "<- getwd();");
		// check R packages
		String[] packages = { "RGoogleAnalytics", "jsonlite", "igraph", "lsa", "httr", "text2vec", "mlapi", "foreach",
				"iterators", "Matrix", "lattice", "digest", "futile.logger", "futile.options", "grid", "lambda.r",
				"formatR", "SnowballC", "codetools", "stringdist", "parallel", "RcppParallel" };
		this.rJavaTranslator.checkPackages(packages);

		// get input parameters
		boolean accessFlag = getAccessBool();
		String description = this.keyValue.get(this.keysToGet[0]);
		List<String> enginesWithAccess = SecurityQueryUtils.getUserEngineIds(this.insight.getUser());

		// source scripts and run find_db
		sb.append(RSyntaxHelper.loadPackages(packages));
		sb.append("setwd(\"" + DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations\");\n");
		sb.append("source(\"db_recom.r\");\n").append("source(\"topic_modelling.r\");\n");
		sb.append("source(\"SemanticSimilarity/lsi_dataitem.r\");\n");
		String output = "output" + Utility.getRandomString(8);
		sb.append(output + " <- find_db(\"dataitem\",\"" + description + "\",0.1,0.5);\n");
		sb.append("na.omit(" + output + ")\n");
		sb.append("setwd(\"" + workDir + "\");");
		this.rJavaTranslator.runR(sb.toString().replace("\\", "/"));

		// receive result data frame back as a List
		String[] headerOrdering = new String[] { "dbid", "dbname", "similarity" };
		List<Object[]> list = this.rJavaTranslator.getBulkDataRow(output, headerOrdering);
		// for each database result, get other meta data
		for (Object[] row : list) {
			String id = row[0] + "";
			// if we require access and we dont have it then skip
			if (accessFlag && !enginesWithAccess.contains(id)) {
				continue;
			}
			String dbName = row[1] + "";
			String sim = (row[2] + "").substring(0, 5);
			boolean access = false;
			String type = "";
			// if we have access get db details
			if (enginesWithAccess.contains(id)) {
				access = true;
				dbName = Utility.getEngine(id).getEngineName();
				type = Utility.getEngine(id).getEngineType() + "";
			}
			// add all data to a list of maps
			HashMap<String, Object> dbDetails = new HashMap<String, Object>();
			dbDetails.put("appName", dbName);
			dbDetails.put("appId", id);
			dbDetails.put("appType", type);
			dbDetails.put("access", access);
			dbDetails.put("sim_score", sim);
			recommendations.add(dbDetails);
		}
		
		
		// garbage clean up
		StringBuilder gc = new StringBuilder();
		gc.append("rm(" + output + ",blend_mgr, data_domain_mgr, read_datamatrix, "
				+ "exec_tfidf, remove_files, fileroot, output, blend_tracking_semantic, "
				+ "get_userdata, dataitem_history, get_dataitem_rating, assign_unique_concepts, "
				+ "populate_ratings, build_sim, cosine_jaccard_sim, cosine_sim, "
				+ "jaccard_sim, apply_tfidf, compute_weight, dataitem_recom_mgr, "
				+ "get_item_recom, get_user_recom, hop_away_recom_mgr, hop_away_mgr, "
				+ "locate_user_communities, drilldown_communities, locate_data_communities, "
				+ "get_items_users, refresh_base, build_data_landmarks, build_dbid_domain, "
				+ "build_query_doc, build_query_tdm, build_tdm, get_similar_doc, "
				+ "lsi_mgr, find_db, match_desc, breakdown);");
		this.rJavaTranslator.runR(gc.toString());
		
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

