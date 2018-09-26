package prerna.util.usertracking.reactors;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Pulls user query information from tracking end point from the widget table
 * Updates the dataitemquery.tsv containing user query information 
 * Creates the .rds files used to generate data recommendations
 */
public class UpdateQueryDataReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = UpdateQueryDataReactor.class.getName();

	@Override
	public NounMetadata execute() {
		init();
		// NEW: Updating "dataquery.tsv" and storing it in working directory
		String FILE_URL = DIHelper.getInstance().getProperty("T_ENDPOINT") + "exportTable/query";
		String FILE_NAME = "dataitem-dataquery.tsv";
		String path = DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations\\";
		try {
			InputStream in = new URL(FILE_URL).openStream();
			Files.copy(in, Paths.get(path + FILE_NAME), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Updating the local file using "dataquery.tsv"
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String rwd = "wd_" + Utility.getRandomString(8);
		StringBuilder rsb = new StringBuilder();
		rsb.append(rwd + "<- getwd();");
		rsb.append("setwd(\"" + baseFolder + "\\R\\Recommendations\");\n");

		// generate script for database recommendations
		rsb.append("source(\"db_recom.r\");");
		rsb.append("source(\"datasemantic.r\");");
		rsb.append("source(\"SemanticSimilarity\\lsi_dataitem.r\");");
		rsb.append("source(\"topic_modelling.r\");");
		String fileroot = baseFolder + "\\R\\Recommendations\\dataitem";
		rsb.append("data_domain_mgr(\"" + fileroot + "\");");
		// set the work directory back to normal
		rsb.append("setwd(" + rwd + ");");
		// garbage collection
		String script = rsb.toString().replace("\\", "/");
		this.rJavaTranslator.runR(script);
		String gc = "rm(" + "\"apply_tfidf\",            \"assign_unique_concepts\","
				+ "\"blend_mgr\",              \"blend_tracking_semantic\","
				+ "\"breakdown\",              \"build_data_landmarks\","
				+ "\"build_dbid_domain\",      \"build_query_doc\"," + "\"build_query_tdm\",        \"build_sim\","
				+ "\"build_tdm\",              \"col2db\"," + "\"col2tbl\",                \"column_doc_mgr_do\","
				+ "\"column_doc_mgr_dopar\",   \"column_lsi_mgr\","
				+ "\"compute_column_desc_sim\",\"compute_entity_sim\","
				+ "\"construct_column_doc\",   \"constructName\","
				+ "\"cosine_jaccard_sim\",     \"create_column_doc\","
				+ "\"data_domain_mgr\",        \"dataitem_history_do\","
				+ "\"dataitem_history_dopar\", \"dataitem_recom_mgr\","
				+ "\"datasemantic_history\",   \"discover_column_desc\","
				+ "\"drilldown_communities\",  \"exec_tfidf\"," + "\"find_db\",                \"get_dataitem_rating\","
				+ "\"get_item_recom\",         \"get_items_users\"," + "\"get_similar_doc\",        \"get_user_recom\","
				+ "\"getSearchURL\",           \"hop_away_mgr\"," + "\"hop_away_recom_mgr\",     \"jaccard_sim\","
				+ "\"locate_data_communities\",\"locate_data_district\"," + "\"locate_user_communities\",\"lsi_mgr\","
				+ "\"match_desc\",             \"populate_ratings\"," + "\"read_datamatrix\",        \"refresh_base\","
				+ "\"refresh_data_mgr\",       \"refresh_semantic_mgr\","
				+ "\"remove_files\",           \"semantic_tracking_mgr\", \"" + rwd + "\")";

		this.rJavaTranslator.runR(gc);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
