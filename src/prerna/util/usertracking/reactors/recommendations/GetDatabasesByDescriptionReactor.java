package prerna.util.usertracking.reactors.recommendations;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.date.SemossDate;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.UserTrackerFactory;

public class GetDatabasesByDescriptionReactor extends AbstractRFrameReactor {

	protected static final String CLASS_NAME = GetDatabasesByDescriptionReactor.class.getName();
	// sets the lower bound threshold for a successful search (i.e. only return
	// results with a similarity score above this limit)
	protected static final String LOW_LIMIT = "lowLimit";
	// sets the range of possible search results lower than the best match
	protected static final String MARGIN = "margin";

	public GetDatabasesByDescriptionReactor() {
		// Default Values-- Access: true, low_limit=0.5, margin = 0.1

		this.keysToGet = new String[] { ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.ACCESS.getKey(),
				LOW_LIMIT, MARGIN };
	}

	@Override
	public NounMetadata execute() {
		// Test with:
		// GetDatabasesByDescription(description=["movie director money"], access=[FALSE], lowLimit=["-1"], margin=["2"]);
		// GetDatabasesByDescription(description=["movie director money"], access=[FALSE]);
		// GetDatabasesByDescription(description=["movie director money"]);
		Logger logger = getLogger(CLASS_NAME);

		// Check to make sure that these files exist before searching
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File lsa = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR + "dataitem-dbid-desc-lsa.rds");
		File vocab = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR + "dataitem-dbid-desc-lsa-vocab.rds");
		File desc = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR + "dataitem-dbid-desc.rds");
		if (!lsa.exists() || !vocab.exists() || !desc.exists()) {
			String message = "Necessary files missing to generate search results. Please run UpdateQueryData() and UpdateSemanticData().";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		init();
		organizeKeys();
		ArrayList<Object> recommendations = new ArrayList<Object>();
		if (UserTrackerFactory.isTracking()) {
			// check R packages
			String[] packages = { "lsa", "text2vec" };
			this.rJavaTranslator.checkPackages(packages);
			String workDir = "wd" + Utility.getRandomString(8);
			StringBuilder sb = new StringBuilder();
			sb.append(workDir + "<- getwd();");
			// get input parameters
			boolean accessFlag = getAccessBool();
			String description = this.keyValue.get(this.keysToGet[0]);
			String lowLimit = "0.5";
			String lowLimitInput = this.keyValue.get(this.keysToGet[2]);
			if (lowLimitInput != null && !lowLimitInput.isEmpty()) {
				lowLimit = lowLimitInput;
			}
			String margin = "0.1";
			String marginInput = this.keyValue.get(this.keysToGet[3]);
			if (marginInput != null && !marginInput.isEmpty()) {
				margin = marginInput;
			}
			List<String> enginesWithAccess = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
			// source scripts and run find_db
			sb.append(RSyntaxHelper.loadPackages(packages));
			sb.append("setwd(\"" + DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations\");");
			sb.append("source(\"db_recom.r\");").append("source(\"topic_modelling.r\");");
			sb.append("source(\"SemanticSimilarity/lsi_dataitem.r\");");
			String output = "output" + Utility.getRandomString(8);
			sb.append(output + " <- find_db(\"dataitem\",\"" + description + "\"," + margin + "," + lowLimit + ");");
			sb.append("na.omit(" + output + ");");
			sb.append("setwd(" + workDir + ");");
			String script = sb.toString().replace("\\", "/");
			this.rJavaTranslator.runR(script);

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
				String dbType = "";
				SemossDate lmDate = null;
				List<String> insights = null;
				// if we have access get db details
				if (enginesWithAccess.contains(id)) {
					access = true;
					dbName = Utility.getDatabase(id).getEngineName();
					dbType = Utility.getDatabase(id).getDatabaseType() + "";
					lmDate = SecurityQueryUtils.getLastModifiedDateForInsightInProject(id);
					//insights = SecurityQueryUtils.getUserInsightsForEngine(this.insight.getUser(), id);
				}
				// add all data to a list of maps
				HashMap<String, Object> dbDetails = new HashMap<String, Object>();
				dbDetails.put("insightName", insights);
				dbDetails.put("app_name", dbName);
				dbDetails.put("app_id", id);
				dbDetails.put("app_type", dbType);
				dbDetails.put("access", access);
				dbDetails.put("sim_score", sim);
				dbDetails.put("lastModified", lmDate);
				recommendations.add(dbDetails);
			}

			// garbage clean up
			StringBuilder gc = new StringBuilder();
			gc.append("rm(" + output + "," + workDir + "," + "apply_tfidf, assign_unique_concepts,"
					+ "blend_mgr, blend_tracking_semantic," + "breakdown, build_data_landmarks,"
					+ "build_dbid_domain, build_query_doc," + "build_query_tdm, build_sim," + "build_tdm,"
					+ "cosine_jaccard_sim, data_domain_mgr," + "dataitem_history, dataitem_recom_mgr,"
					+ "drilldown_communities, exec_tfidf," + "find_db, get_dataitem_rating,"
					+ "get_item_recom, get_items_users," + "get_similar_doc, get_user_recom,"
					+ "get_userdata, hop_away_mgr," + "hop_away_recom_mgr, jaccard_sim,"
					+ "locate_data_communities, locate_user_communities," + "lsi_mgr, match_desc,"
					+ "outputa8VNk3h38, populate_ratings," + "read_datamatrix, refresh_base," + "remove_files);");
			this.rJavaTranslator.runR(gc.toString());
		}
		return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.RECOMMENDATION);
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
