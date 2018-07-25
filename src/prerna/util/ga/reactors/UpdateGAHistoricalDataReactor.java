package prerna.util.ga.reactors;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

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
		String[] packages = {"RGoogleAnalytics", "httr", "data.table", "jsonlite", "plyr", "lubridate", "curl"};
		this.rJavaTranslator.checkPackages(packages);
	
		// get start day from inputs, date range default is 10 days
		int dateRange = 10;
		try {
			dateRange = Integer.parseInt(this.keyValue.get(this.keysToGet[0]));
		} catch (NumberFormatException e) {
			// do nothing
		}
		
		// format dates for range
		String todayDate = LocalDate.now().toString();
		String startDate = LocalDate.now().minus(dateRange, ChronoUnit.DAYS).toString();
        String wd = this.rJavaTranslator.getString("getwd()");
        String wrkDir = "setwd(\"" + DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations\");\n";
        wrkDir = wrkDir.replace("\\", "/");
		this.rJavaTranslator.runR(wrkDir);
        
		// generate script to run viz_tracking.r script
		String userDf = "user_" + Utility.getRandomString(8);
		String historyDf = "hist_" + Utility.getRandomString(8);
		String rDirectory = DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations";
		String script = "source(\"" + rDirectory + "\\viz_tracking.r\");"
				+ userDf + "<-get_userdata(\"" + startDate + "\",\"" + todayDate + "\", \"" + rDirectory + "\\token_file\");"
				+ historyDf + "<-viz_history(" + userDf + ");"
				+ "write.csv(" + historyDf + ",file=\"" + rDirectory + "\\historicalData\\viz_user_history.csv\",row.names=FALSE,na=\"\");";
		script = script.replace("\\", "/");
		this.rJavaTranslator.runR(script);
		
		// generate script for database recommendations
		String userDf2 = "user_" + Utility.getRandomString(8);
		script = "rm(get_userdata, " + userDf + "); source(\"" + rDirectory + "\\db_recom.r\"); " + userDf2 + "<-get_userdata(\"" + startDate + "\",\"" + todayDate + "\", \"" + rDirectory + "\\token_file\"); fileroot<-\"dataitem\"; ";
		script = script.replace("\\", "/");
		this.rJavaTranslator.runR(script);
		String run = "refresh_base(" + userDf2 + ",fileroot)";
		this.rJavaTranslator.runR(run);

		// set working directory back to normal
		this.rJavaTranslator.runR("setwd('" + wd + "');\n");
		
		// garbage cleanup
		String gc = "rm(fileroot, viz_recom_mgr, restore_datatype, get_reference, blend_tracking_semantic, get_userdata, dataitem_history, get_dataitem_rating, assign_unique_concepts, populate_ratings, build_sim, cosine_jaccard_sim, cosine_sim, jaccard_sim, apply_tfidf, compute_weight, dataitem_recom_mgr, get_item_recom, get_user_recom, hop_away_recom_mgr, hop_away_mgr, locate_user_communities, drilldown_communities, locate_data_communities, get_items_users, refresh_base," + userDf + ", " + historyDf + ", viz_history, viz_recom, get_userdata);";
		this.rJavaTranslator.runR(gc);
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
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
