package prerna.util.ga.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class VisualizationRecommendationReactor extends AbstractRFrameReactor{
	public static final String MAX_RECOMMENDATIONS = "max";
	
	public VisualizationRecommendationReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), MAX_RECOMMENDATIONS};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		
		// check if packages are installed
		String[] packages = {"dplyr", "RGoogleAnalytics", "httr", "data.table", "jsonlite", "plyr", "RJSONIO", "lubridate"};
		this.rJavaTranslator.checkPackages(packages);
		
		// convert qs to physical names
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		String[] qsHeaders = frame.getQsHeaders();
		OwlTemporalEngineMeta meta = frame.getMetaData();
		
		// prep script components
		StringBuilder builder = new StringBuilder();
		String inputFrame = "inputFrame." + Utility.getRandomString(8);
		String dfStart = "library(RJSONIO); library(plyr); " + inputFrame + " <- data.frame(dbname = character(), tblname = character(), colname = character(), stringsAsFactors = FALSE);";
		
		// iterate selectors and update data table builder section of R script
		Map<String, String> aliasHash = new HashMap<String, String>();
		int rowCount = 1;
		for (int i = 0; i < qsHeaders.length; i++) {
			String alias = (qsHeaders[i].split("__"))[1];			
			String name = qsHeaders[i];
			List<String[]> dbInfo = meta.getDatabaseInformation(name);
			int size = dbInfo.size();
			for (int j = 0; j < size; j++) {
				String[] engineQs = dbInfo.get(0);
				String db = engineQs[0];
				String conceptProp = engineQs[1];
				String table = conceptProp;
				String column = QueryStruct2.PRIM_KEY_PLACEHOLDER;
				if (conceptProp.contains("__")) {
					String[] conceptPropSplit = conceptProp.split("__");
					table = conceptPropSplit[0];
					column = conceptPropSplit[1];
				}
				// add all columns and logical names
				// to alias hash so we can look up alias later
				aliasHash.put(column + "_" + table + "_" + db, alias);
				List<String> logicalNames = MasterDatabaseUtility.getLogicalNames(db, table);
				// build dataframe of all columns in semoss frame plus logical names
				for (int k = 0 ; k < logicalNames.size() ; k++){
					aliasHash.put(logicalNames.get(k) + "_" + table + "_" + db, alias);
					builder.append(inputFrame)
					.append("[")
					.append(rowCount)
					.append(", ] <- c( \"")
					.append(db).append("\", \"")
					.append(table)
					.append("\", \"")
					.append(logicalNames.get(k))
					.append("\");");
					rowCount++;
				}
				builder.append(inputFrame)
				.append("[")
				.append(rowCount)
				.append(", ] <- c( \"")
				.append(db).append("\", \"")
				.append(table)
				.append("\", \"")
				.append(column)
				.append("\");");
				rowCount++;
			}
		}
       
		// add the execute predict viz and convert to json piece to script
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String outputJson = "json_" + Utility.getRandomString(8);
		String recommend = "rec_" + Utility.getRandomString(8);
		String historicalDf = "df_" + Utility.getRandomString(8);
		String maxRecommendations = this.keyValue.get(this.keysToGet[1]);
		if (maxRecommendations == null){
			maxRecommendations = "5";
		}
		
		String runPredictScripts = "source(\"" + baseFolder 
				+ "\\R\\Recommendations\\viz_tracking.r\") ; "
				+ historicalDf +"<-read.csv(\"" + baseFolder
				+ "\\R\\Recommendations\\historicalData\\viz_user_history.csv\") ;"
				+ recommend + "<-viz_recom(" + historicalDf + "," + inputFrame + ", \"Grid\", " + maxRecommendations + "); " + outputJson + " <-toJSON(" + recommend + ", byrow = TRUE, colNames = TRUE);";
		runPredictScripts = runPredictScripts.replace("\\", "/");
		
		
		// combine script pieces and execute in R
		String script = dfStart + builder + runPredictScripts;
		this.rJavaTranslator.runR(script);
		
		// receive json string from R
		String json = this.rJavaTranslator.getString(outputJson + ";");
		Map recommendations = new HashMap<String, HashMap<String ,String>>();
		Gson gson = new Gson();
		if(json == null) {
			return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}
		ArrayList<Map<String,String>> myList = gson.fromJson(json, new TypeToken<ArrayList<HashMap<String,String>>>(){}.getType());
		for (int i = 0 ; i < myList.size() ; i++){
			// get all values from R json
			Map map = new HashMap<String ,String>();
			String dbName = myList.get(i).get("dbname");
			String tblName = myList.get(i).get("tblname");
			String colName = myList.get(i).get("colname");
			String component = myList.get(i).get("component");
			String chart = myList.get(i).get("chart");
			String weight = myList.get(i).get("weight");
			String columnAlias = aliasHash.get(colName + "_" + tblName + "_" + dbName);
			// cant recommend something thats not in the current frame
			if (columnAlias != null) {
				// String chartWeight = myList.get(i).get("chartweight");
				if (recommendations.containsKey(chart)) {
					map = (HashMap) recommendations.get(chart);
					map.put(columnAlias, component);
					recommendations.put(chart, map);
				} else {
					map.put("weight", weight);
					map.put(columnAlias, component);
					recommendations.put(chart, map);
				}
			}
		}
		
		// garbage cleanup -- R script might already do this
		String gc = "rm(" + outputJson + ", " + recommend + ", " + historicalDf + ", " + inputFrame + ", " + "viz_history, viz_recom, get_userdata);";
		this.rJavaTranslator.runR(gc);
		
		return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(MAX_RECOMMENDATIONS)) {
			return "The maximum amount of visualization recommendations returned.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
