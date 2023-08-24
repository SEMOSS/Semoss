package prerna.util.usertracking.reactors;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.UserTrackerFactory;

/**
 * Pulls column descriptions from semantic table Generates the .rds files to
 * create recommendations and search
 */
public class UpdateSemanticDataReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = UpdateSemanticDataReactor.class.getName();

	@Override
	public NounMetadata execute() {
		if (UserTrackerFactory.isTracking()) {
			init();
			Logger logger = getLogger(CLASS_NAME);
			// NEW: Updating "datasemantic.tsv" and storing it in working
			// directory
			String[] packages = new String[] { "mlapi", "text2vec", "lattice", "foreach", "grid", "lambda.r",
					"futile.logger", "iterators", "RcppParallel", "digest", "Matrix", "lsa", "formatR",
					"futile.options", "codetools", "SnowballC", "compiler" };
			this.rJavaTranslator.checkPackages(packages);
			String extension = "?databases=";
			// read datadistrict file
			String[] relevantEngines = new String[10000]; //arbitrary size.. but should not have more than 10,000 relevant databases
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			File f = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR + "dataitem-datadistrict.rds");

			if (f.exists()) {
				StringBuilder sb = new StringBuilder();
				sb.append("output <- readRDS(\"" + f.getPath() + "\");\n");
				sb.append("output;");
				String script = sb.toString().replace("\\", "/");
				relevantEngines = this.rJavaTranslator.getStringArray(script);
			} else {
				String message = "Unable to access required dataitem-datadistrict.rds file to update semantic data.";
				NounMetadata retNoun = new NounMetadata(false, PixelDataType.BOOLEAN);
				retNoun.addAdditionalReturn(
						new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				return retNoun;
			}

			// generate list of databases and store it into an array
			for (String row : relevantEngines) {
				String[] parsed = row.split("\\$");
				extension += parsed[0] + ";";
			}
			
			List<String> enginesWithAccess = SecurityEngineUtils.getFullUserEngineIds(this.insight.getUser());
			for (String row : enginesWithAccess) {
				extension+= row + ";";
			}

			String FILE_URL = DIHelper.getInstance().getProperty("T_ENDPOINT") + "exportTable/semantic" + extension;
			String FILE_NAME = "dataitem-datasemantic.tsv";
			String path = DIHelper.getInstance().getProperty("BaseFolder") + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR;

			// server is available: Begin Method
			logger.info("Cacheing data semantic file");
			long start = System.currentTimeMillis();
			Utility.copyURLtoFile(FILE_URL, path + FILE_NAME);
			long end = System.currentTimeMillis();
			logger.info("Cacheing time " + (end - start) + " ms");

			File semanticData = new File(path + FILE_NAME);
			if (!semanticData.exists()) {
				String message = "Unable to connect to the server database for data query and visualization information.";
				NounMetadata retNoun = new NounMetadata(false, PixelDataType.BOOLEAN);
				retNoun.addAdditionalReturn(
						new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				return retNoun;
			} else {
				// Updating the local file using "datasemantic.tsv"
				String rwd = "wd_" + Utility.getRandomString(8);
				StringBuilder rsb = new StringBuilder();
				rsb.append(rwd + "<- getwd();");
				rsb.append("setwd(\"" + baseFolder + "\\R\\Recommendations\");");

				// generate script for database recommendations
				rsb.append("source(\"db_recom.r\");");
				rsb.append("source(\"datasemantic.r\");");
				rsb.append("source(\"SemanticSimilarity\\lsi_dataitem.r\");");
				rsb.append("source(\"topic_modelling.r\");");
				String fileroot = baseFolder + "\\R\\Recommendations\\dataitem";
				rsb.append("refresh_semantic_mgr(\"" + fileroot + "\");");
				// set the work directory back to normal
				rsb.append("setwd(" + rwd + ");");
				// garbage collection
				String script = rsb.toString().replace("\\", "/");
				this.rJavaTranslator.runR(script);
				String gc = "rm(\"apply_tfidf\",                        \"assign_unique_concepts\","
						+ "\"blend_mgr\",                          \"blend_tracking_semantic\","
						+ "\"breakdown\",                          \"build_data_landmarks\","
						+ "\"build_dbid_domain\",                  \"build_query_doc\","
						+ "\"build_query_tdm\",                    \"build_sim\","
						+ "\"build_tdm\",                          \"col2db\","
						+ "\"col2tbl\",                            \"column_doc_mgr_do\","
						+ "\"column_doc_mgr_dopar\",               \"column_lsi_mgr\","
						+ "\"compute_column_desc_sim\",            \"compute_entity_sim\","
						+ "\"construct_column_doc\","
						+ "\"constructName\",                      \"cosine_jaccard_sim\","
						+ "\"create_column_doc\",                  \"data_domain_mgr\","
						+ "\"dataitem_history_do\",                \"dataitem_history_dopar\","
						+ "\"dataitem_recom_mgr\",                 \"datasemantic_history\","
						+ "\"discover_column_desc\",               \"drilldown_communities\","
						+ "\"exec_tfidf\",                         \"find_db\","
						+ "\"get_dataitem_rating\",                \"get_item_recom\","
						+ "\"get_items_users\",                    \"get_similar_doc\","
						+ "\"get_user_recom\",                     \"getSearchURL\","
						+ "\"hop_away_mgr\",                       \"hop_away_recom_mgr\","
						+ "\"jaccard_sim\",                        \"locate_data_communities\","
						+ "\"locate_data_district\",               \"locate_user_communities\","
						+ "\"lsi_mgr\",                            \"match_desc\","
						+ "\"populate_ratings\",                   \"read_datamatrix\","
						+ "\"refresh_base\",                       \"refresh_data_mgr\","
						+ "\"refresh_semantic_mgr\",               \"remove_files\"," + "\"semantic_tracking_mgr\", \""
						+ rwd + "\");";
				this.rJavaTranslator.runR(gc);
				return new NounMetadata(true, PixelDataType.BOOLEAN);
			}
		}
		return new NounMetadata(false, PixelDataType.BOOLEAN);
	}

}
