package prerna.util.ga.reactors;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class UpdateGAHistoricalDataReactor extends AbstractRFrameReactor {

	public static final String DATE_RANGE = "range";
	
	public UpdateGAHistoricalDataReactor() {
		this.keysToGet = new String[]{DATE_RANGE};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		
		// check if packages are installed
		String[] packages = {"RGoogleAnalytics", "httr", "data.table", "jsonlite", "plyr", "lubridate", "curl", "lsa", "LSAfun", "text2vec", "stringr", "stringdist"};
		this.rJavaTranslator.checkPackages(packages);
	
		// get start day from inputs, date range default is 10 days
		int dateRange = 10;
		try {
			dateRange = Integer.parseInt(this.keyValue.get(this.keysToGet[0]));
		} catch (NumberFormatException e) {
			// do nothing
		}
		
		this.rJavaTranslator.runR(updateGAHistoricalDataRSyntax(dateRange));
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	/**
	 * Generate the script to update Google Analytics Historical data
	 * @param dateRange
	 * @return
	 */
	public String updateGAHistoricalDataRSyntax(int dateRange) {
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		// format dates for range
		String todayDate = LocalDate.now().toString();
		String startDate = LocalDate.now().minus(dateRange, ChronoUnit.DAYS).toString();
		String rwd = "wd_" + Utility.getRandomString(8);
		StringBuilder rsb = new StringBuilder();
		rsb.append(rwd + "<- getwd();");
		rsb.append("setwd(\"" + baseFolder + "\\R\\Recommendations\");\n");
		// generate script to run viz_tracking.r script
		String userDf = "user_" + Utility.getRandomString(8);
		String historyDf = "hist_" + Utility.getRandomString(8);
		String rDirectory = baseFolder + "\\R\\Recommendations";
		rsb.append("source(\"" + rDirectory + "\\viz_tracking.r\");" );
		rsb.append(userDf + "<-get_userdata(\"" + startDate + "\",\"" + todayDate + "\", \"" + rDirectory + "\\token_file\");");
		rsb.append(historyDf + "<-viz_history(" + userDf + ");" );
		rsb.append(RSyntaxHelper.getFWriteSyntax(historyDf, rDirectory + "\\historicalData\\viz_user_history.csv"));
		// generate script for database recommendations
		rsb.append("source(\"" + rDirectory + "\\db_recom.r\");");
		rsb.append("source(\"" + rDirectory + "\\datasemantic.r\");");
		rsb.append(" source(\"" + rDirectory + "\\SemanticSimilarity\\lsi_dataitem.r\");");
		rsb.append("data_domain_mgr(\"" + startDate + "\",\"" + todayDate + "\", \"dataitem\");");
		// set the work directory back to normal
		rsb.append("setwd(" + rwd + ");");
		// garbage collection
		rsb.append("rm(" + userDf + ", " + historyDf + "," + rwd + ",apply_tfidf, assign_unique_concepts,blend_mgr,"
				+ "blend_tracking_semantic,build_query_doc,build_query_tdm,build_sim,build_tdm,"
				+ "col2db,col2tbl,column_lsi_mgr,compute_column_desc_sim,compute_entity_sim,"
				+ "compute_weight,con,cosine_jaccard_sim,cosine_sim,data_domain_mgr,"
				+ "dataitem_history,dataitem_recom_mgr,datasemantic_history,drilldown_communities"
				+ ",exec_tfidf,get_dataitem_rating,get_item_recom,get_items_users,"
				+ "get_reference,get_similar_doc,get_user_recom,get_userdata,"
				+ "hop_away_mgr,hop_away_recom_mgr,jaccard_sim,locate_data_communities,"
				+ "locate_user_communities,lsi_mgr,match_desc,populate_ratings,"
				+ "read_datamatrix,refresh_base,remove_files,restore_datatype,"
				+ "viz_history,viz_recom,viz_recom_mgr)");
		return rsb.toString().replace("\\", "/");
	}
	
	///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(DATE_RANGE)) {
			return "The amount of days of historical data to collect.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
