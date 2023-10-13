package prerna.util.usertracking.reactors;

import java.io.File;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.UserTrackerFactory;

/**
 * Pulls user query information from tracking end point from the widget table
 * Updates the dataitemquery.tsv containing user query information Creates the
 * .rds files used to generate data recommendations
 */
public class UpdateQueryDataReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = UpdateQueryDataReactor.class.getName();

	@Override
	public NounMetadata execute() {
		if (UserTrackerFactory.isTracking()) {
			init();
			String[] packages = new String[] { "igraph", "doParallel", "foreach", "parallel", "iterators", "lsa",
					"SnowballC", "codetools", "compiler" };
			this.rJavaTranslator.checkPackages(packages);
			Logger logger = getLogger(CLASS_NAME);

			//Getting the user info
			User user = this.insight.getUser();
			String userName = "";
			// Updating "dataquery.tsv" and storing it in working directory
			String FILE_URL = DIHelper.getInstance().getProperty("T_ENDPOINT") + "exportTable/query";
			String FILE_NAME = "dataitem-dataquery.tsv";
			String path = DIHelper.getInstance().getProperty("BaseFolder") + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR;

			logger.info("Cacheing data query file");
			long start = System.currentTimeMillis();
			Utility.copyURLtoFile(FILE_URL, path + FILE_NAME);
			long end = System.currentTimeMillis();
			logger.info("Cacheing time " + (end - start) + " ms");

			File queryData = new File(path + FILE_NAME);
			// check to make sure server database is accessible
			// check to see if the server is accessible -- end the method if not
			if (!queryData.exists()) {
				String message = "Unable to connect to the server database for data query and visualization information.";
				NounMetadata retNoun = new NounMetadata(false, PixelDataType.BOOLEAN);
				retNoun.addAdditionalReturn(new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				return retNoun;
			} else {
				// Visualization Data
				logger.info("Cacheing data visualization  file");
				start = System.currentTimeMillis();
				FILE_URL = DIHelper.getInstance().getProperty("T_ENDPOINT") + "exportTable/visualization";
				FILE_NAME = "dataitem-visualization.tsv";
				path = DIHelper.getInstance().getProperty("BaseFolder") + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR;
				Utility.copyURLtoFile(FILE_URL, path + FILE_NAME);
				end = System.currentTimeMillis();
				logger.info("Cacheing time " + (end - start) + " ms");

				// Updating the local file using "dataquery.tsv"
				String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
				String rwd = "wd_" + Utility.getRandomString(8);
				StringBuilder rsb = new StringBuilder();
				rsb.append(rwd + "<- getwd();");
				rsb.append("setwd(\"" + baseFolder + "\\R\\Recommendations\");");

				// generate script for database recommendations
				rsb.append("source(\"db_recom.r\");");
				rsb.append("source(\"datasemantic.r\");");
				rsb.append("source(\"SemanticSimilarity\\lsi_dataitem.r\");");
				rsb.append("source(\"topic_modelling.r\");");
				rsb.append("source(\"viz_recom.r\");");
				String fileroot = baseFolder + "\\R\\Recommendations\\dataitem";
				if(user != null) {
					userName = user.getAccessToken(user.getLogins().get(0)).getId();
					rsb.append("refresh_data_mgr(\"" + fileroot + "\", \"" + userName + "\");");
				}else {
					String message = "Unable to generage datadistrict file. Please login and run UpdateQueryData() again for enhanced data.";
					logger.info(message);
					rsb.append("refresh_data_mgr(\"" + fileroot + "\");");
				}
				rsb.append("viz_history(\"" + fileroot + "\");");
				// set the work directory back to normal
				rsb.append("setwd(" + rwd + ");");
				// garbage collection
				String script = rsb.toString().replace("\\", "/");
				this.rJavaTranslator.runR(script);
				String gc = "rm(" + "\"apply_tfidf\",            \"assign_unique_concepts\","
						+ "\"blend_mgr\",              \"blend_tracking_semantic\","
						+ "\"breakdown\",              \"build_data_landmarks\","
						+ "\"build_dbid_domain\",      \"build_query_doc\","
						+ "\"build_query_tdm\",        \"build_sim\"," + "\"build_tdm\",              \"col2db\","
						+ "\"col2tbl\",                \"column_doc_mgr_do\","
						+ "\"column_doc_mgr_dopar\",   \"column_lsi_mgr\","
						+ "\"compute_column_desc_sim\",\"compute_entity_sim\","
						+ "\"construct_column_doc\",   \"constructName\","
						+ "\"cosine_jaccard_sim\",     \"create_column_doc\","
						+ "\"data_domain_mgr\",        \"dataitem_history_do\","
						+ "\"dataitem_history_dopar\", \"dataitem_recom_mgr\","
						+ "\"datasemantic_history\",   \"discover_column_desc\","
						+ "\"drilldown_communities\",  \"exec_tfidf\","
						+ "\"find_db\",                \"get_dataitem_rating\","
						+ "\"get_item_recom\",         \"get_items_users\","
						+ "\"get_similar_doc\",        \"get_user_recom\","
						+ "\"getSearchURL\",           \"hop_away_mgr\","
						+ "\"hop_away_recom_mgr\",     \"jaccard_sim\","
						+ "\"locate_data_communities\",\"locate_data_district\","
						+ "\"locate_user_communities\",\"lsi_mgr\","
						+ "\"match_desc\",             \"populate_ratings\","
						+ "\"read_datamatrix\",        \"refresh_base\","
						+ "\"refresh_data_mgr\",       \"refresh_semantic_mgr\","
						+ "\"remove_files\",           \"semantic_tracking_mgr\", \"" + rwd + "\")";

				this.rJavaTranslator.runR(gc);
				return new NounMetadata(true, PixelDataType.BOOLEAN);
			}
		}
		return new NounMetadata(false, PixelDataType.BOOLEAN);
	}

}
