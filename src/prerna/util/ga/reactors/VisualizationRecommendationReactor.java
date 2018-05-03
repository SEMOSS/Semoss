package prerna.util.ga.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class VisualizationRecommendationReactor extends AbstractRFrameReactor {
	public static final String MAX_RECOMMENDATIONS = "max";

	/**
	 * This reactor generates visualization recommendations based on the data in the current frame. 
	 * It runs 2 R functions, both using historical data tracked in google analytics. 
	 * One script recommends based on exact matches and semantic matches of columns and the other uses data types, results are combined.
	 */
	
	public VisualizationRecommendationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TASK.getKey(), MAX_RECOMMENDATIONS };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();

		// check if packages are installed
		String[] packages = { "RGoogleAnalytics", "httr", "data.table", "jsonlite", "plyr", "RJSONIO", "lubridate" };
		this.rJavaTranslator.checkPackages(packages);

		// convert qs to physical names
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		if (frame == null) {
			return new NounMetadata(new HashMap(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_RECOMMENDATION);
		}
		String[] qsHeaders = frame.getQsHeaders();
		OwlTemporalEngineMeta meta = frame.getMetaData();

		// prep script components
		StringBuilder builder = new StringBuilder();
		String inputFrame = "inputFrame." + Utility.getRandomString(8);
		String dfStart = inputFrame + " <- data.frame(reference1 = character(), reference2 = character(), reference3 = integer(), stringsAsFactors = FALSE);";

		// iterate selectors and update data table builder section of R script
		Map<String, String> aliasHash = new HashMap<String, String>();
		int rowCount = 1;
		for (int i = 0; i < qsHeaders.length; i++) {
			String name = qsHeaders[i];
			String alias = name;
			if(alias.contains("__")) {
				alias = name.split("__")[1];
			}
			List<String[]> dbInfo = meta.getDatabaseInformation(name);
			int size = dbInfo.size();
			for (int j = 0; j < size; j++) {
				String[] engineQs = dbInfo.get(0);
				if(engineQs.length == 1) {
					// we do not know the source of this column
					continue;
				}
				String db = engineQs[0];
				String conceptProp = engineQs[1];
				String table = conceptProp;
				String column = SelectQueryStruct.PRIM_KEY_PLACEHOLDER;
				if (conceptProp.contains("__")) {
					String[] conceptPropSplit = conceptProp.split("__");
					table = conceptPropSplit[0];
					column = conceptPropSplit[1];
				}

				// add row to data type R df used for offline recommendations
				String dataType = meta.getHeaderTypeAsString(name);
				
				// get unique column values
				IEngine engine = Utility.getEngine(db);
				ENGINE_TYPE type = engine.getEngineType();
				RDFFileSesameEngine owlEngine = null;
				if (type.equals(ENGINE_TYPE.RDBMS)) {
					RDBMSNativeEngine eng = (RDBMSNativeEngine) engine;
					owlEngine = eng.getBaseDataEngine();
				} else if (type.equals(ENGINE_TYPE.TINKER)) {
					TinkerEngine eng = (TinkerEngine) engine;
					owlEngine = eng.getBaseDataEngine();
				} else if (type.equals(ENGINE_TYPE.JENA)) {
					RDFFileSesameEngine eng = (RDFFileSesameEngine) engine;
					owlEngine = eng.getBaseDataEngine();
				}

				// get unique values for string columns, if it doesnt exist
				// or isnt a string then it is defaulted to zero
				long uniqueValues = 0;
				if (dataType != null && dataType.equals("STRING")) {
					String queryCol = column;
					// prim key placeholder cant be queried in the owl
					// so we convert it back to the display name of the concept
					if (column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
						queryCol = table;
					}
					String uniqueValQuery = "SELECT DISTINCT ?concept ?unique WHERE "
							+ "{ BIND(<http://semoss.org/ontologies/Concept/" + queryCol + "/" + table
							+ "> AS ?concept)"
							+ "{?concept <http://semoss.org/ontologies/Relation/Contains/UNIQUE> ?unique}}";
					IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(owlEngine, uniqueValQuery);
					while (it.hasNext()) {
						Object[] row = it.next().getValues();
						uniqueValues = Long.parseLong(row[1].toString());
					}
				}

				builder.append(inputFrame).append("[").append(rowCount).append(", ] <- c( \"").append(db).append("$")
						.append(table).append("$").append(column).append("\"").append(", \"").append(dataType)
						.append("\" , ").append(uniqueValues).append(");");

				// add column alias hash so we can look up alias later
				aliasHash.put(column + "_" + table + "_" + db, alias);
				rowCount++;
			}
		}

		// add the execute predict viz and convert to json piece to script
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String outputJson = "json_" + Utility.getRandomString(8);
		String recommend = "rec_" + Utility.getRandomString(8);
		String historicalDf = "df_" + Utility.getRandomString(8);
		String maxRecommendations = this.keyValue.get(this.keysToGet[1]);
		if (maxRecommendations == null) {
			maxRecommendations = "5";
		}

		String runPredictScripts = "source(\"" + baseFolder + "\\R\\Recommendations\\viz_tracking.r\") ; "
				+ historicalDf + "<-read.csv(\"" + baseFolder + "\\R\\Recommendations\\historicalData\\viz_user_history.csv\") ;" 
				+ recommend + "<-viz_recom_mgr(" + historicalDf + "," + inputFrame + ", \"Grid\", 5); " 
				+ "library(jsonlite);" + outputJson + " <-toJSON(" + recommend + ", byrow = TRUE, colNames = TRUE);"; 
		runPredictScripts = runPredictScripts.replace("\\", "/");

		// combine script pieces and execute in R
		String script = dfStart + builder + runPredictScripts;
		System.out.println(script);
		System.out.println(script);
		this.rJavaTranslator.runR(script);

		// receive json string from R
		String json = this.rJavaTranslator.getString(outputJson + ";");
		Map recommendations = new HashMap<String, HashMap<String, String>>();
		Gson gson = new Gson();
		
		// garbage cleanup -- R script might already do this
		String gc = "rm(" + outputJson + ", " + recommend + ", " + historicalDf + ", " + "viz_history, viz_recom, get_userdata, get_reference, viz_recom_offline, viz_recom_mgr);";

		this.rJavaTranslator.runR(gc);
		// if recommendations fail return empty map
		if (json == null) {
			return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_RECOMMENDATION);
		}
		// converting physical column names to frame aliases
		ArrayList<Map<String, String>> myList = gson.fromJson(json, new TypeToken<ArrayList<HashMap<String, String>>>(){}.getType());
		for (int i = 0; i < myList.size(); i++) {
			// get all values from R json
			Map map = new HashMap<String, String>();
			String dbName = myList.get(i).get("dbname");
			String tblName = myList.get(i).get("tblname");
			String colName = myList.get(i).get("colname");
			String component = myList.get(i).get("component");
			String chart = myList.get(i).get("chart");
			String weight = myList.get(i).get("weight");
			String columnAlias = aliasHash.get(colName + "_" + tblName + "_" + dbName);

			// make sure column is in current frame then add to recommendation map
			if (columnAlias != null) {
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
		return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_RECOMMENDATION);
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
