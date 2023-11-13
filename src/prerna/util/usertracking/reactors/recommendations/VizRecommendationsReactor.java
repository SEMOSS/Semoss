package prerna.util.usertracking.reactors.recommendations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class VizRecommendationsReactor extends AbstractRFrameReactor {
	
	private static final Logger classLogger = LogManager.getLogger(VizRecommendationsReactor.class);
	
	public static final String MAX_RECOMMENDATIONS = "max";

	/**
	 * This reactor generates visualization recommendations based on the data in
	 * the current frame. It runs 2 R functions, both using historical data
	 * tracked in server database One script recommends based on exact matches
	 * and semantic matches of columns and the other uses data types, results
	 * are combined.
	 */

	public VizRecommendationsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.TASK.getKey(), MAX_RECOMMENDATIONS };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (user == null) {
			String message = "Please login to enable recommendation features.";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		File desc = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR + "dataitem-user-history.rds");
		if (!desc.exists()) {
			String message = "Necessary files missing to generate search results. Please run UpdateQueryData().";
			NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		init();
		organizeKeys();

		// check if packages are installed
		String[] packages = { "RGoogleAnalytics", "httr", "jsonlite", "plyr", "RJSONIO", "lubridate" };
		this.rJavaTranslator.checkPackages(packages);

		// convert qs to physical names
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		if (frame == null) {
			return new NounMetadata(new HashMap(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.VIZ_RECOMMENDATION);
		}
		String[] qsHeaders = frame.getQsHeaders();
		OwlTemporalEngineMeta meta = frame.getMetaData();

		// prep script components
		StringBuilder builder = new StringBuilder();
		String inputFrame = "inputFrame." + Utility.getRandomString(8);
		builder.append(inputFrame
				+ " <- data.frame(reference1 = character(), reference2 = character(), reference3 = integer(), stringsAsFactors = FALSE);");

		// iterate selectors and update data table builder section of R script
		Map<String, String> aliasHash = new HashMap<String, String>();
		int rowCount = 1;
		for (int i = 0; i < qsHeaders.length; i++) {
			String name = qsHeaders[i];
			String alias = name;
			if (alias.contains("__")) {
				alias = name.split("__")[1];
			}
			List<String[]> dbInfo = meta.getDatabaseInformation(name);
			int size = dbInfo.size();
			for (int j = 0; j < size; j++) {
				String[] engineQs = dbInfo.get(0);
				if (engineQs.length == 1) {
					// we do not know the source of this column
					continue;
				}
				String db = engineQs[0];
				String dbname = SecurityEngineUtils.getEngineAliasForId(db);
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
				IDatabaseEngine engine = Utility.getDatabase(db);
				// get unique values for string columns, if it doesnt exist
				long uniqueValues = 0;
				String queryCol = column;
				// prim key placeholder cant be queried in the owl
				// so we convert it back to the display name of the concept
				if (column.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)) {
					queryCol = table;
				}
				String uniqueValQuery = "SELECT DISTINCT ?concept ?unique WHERE "
						+ "{ BIND(<http://semoss.org/ontologies/Concept/" + queryCol + "/" + table + "> AS ?concept)"
						+ "{?concept <http://semoss.org/ontologies/Relation/Contains/UNIQUE> ?unique}}";
				IRawSelectWrapper it = null;
				try {
					it = engine.getOWLEngineFactory().getReadOWL().query(uniqueValQuery);
					while (it.hasNext()) {
						Object[] row = it.next().getValues();
						uniqueValues = Long.parseLong(row[1].toString());
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(it != null) {
						try {
							it.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}

				builder.append(inputFrame).append("[").append(rowCount).append(", ] <- c( \"").append(db).append("$").append(dbname).append("$")
						.append(table).append("$").append(column).append("\"").append(", \"").append(dataType)
						.append("\" , ").append(uniqueValues).append(");");

				// add column alias hash so we can look up alias later
				aliasHash.put(column + "_" + table + "_" + db, alias);
				rowCount++;
			}
		}

		// add the execute predict viz and convert to json piece to script
		String outputJson = "json_" + Utility.getRandomString(8);
		String recommend = "rec_" + Utility.getRandomString(8);
		String historicalDf = "df_" + Utility.getRandomString(8);
		String maxRecommendations = this.keyValue.get(this.keysToGet[1]);
		if (maxRecommendations == null) {
			maxRecommendations = "5";
		}
		builder.append("origDir <- getwd();");
		builder.append("setwd(\"" + baseFolder + "\\R\\Recommendations\");"); 
		builder.append("source(\"viz_recom.r\") ; "); 
		builder.append(recommend + "<-viz_recom_mgr(\"dataitem\", " + inputFrame + ", \"Grid\", 5); "); 
		builder.append("library(jsonlite);");
		builder.append(outputJson + " <-toJSON(" + recommend + ", byrow = TRUE, colNames = TRUE);");
		builder.append("setwd(origDir);");
		String script = builder.toString().replace("\\", "/");
		this.rJavaTranslator.runR(builder.toString().replace("\\", "/"));

		// receive json string from R
		String json = this.rJavaTranslator.getString(outputJson + ";");
		Map recommendations = new HashMap<String, HashMap<String, String>>();

		// garbage cleanup -- R script might already do this
		String gc = "rm(" + outputJson + ", " + recommend + ", " + historicalDf + ", "
				+ "viz_history, viz_recom, get_userdata, get_reference, viz_recom_offline, viz_recom_mgr);";

		this.rJavaTranslator.runR(gc);
		// if recommendations fail return empty map
		if (json == null) {
			return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.VIZ_RECOMMENDATION);
		}

		// converting physical column names to frame aliases
		Gson gson = new Gson();
		ArrayList<Map<String, String>> myList = gson.fromJson(json,
				new TypeToken<ArrayList<HashMap<String, String>>>() {
				}.getType());
		for (int i = 0; i < myList.size(); i++) {
			// get all values from R json
			Map map = new HashMap<String, String>();
			String dbid = myList.get(i).get("dbid");
			String dbName = myList.get(i).get("dbname");
			String tblName = myList.get(i).get("tblname");
			String colName = myList.get(i).get("colname");
			String component = myList.get(i).get("component");
			String chart = myList.get(i).get("chart");
			String weight = myList.get(i).get("weight");
			String columnAlias = aliasHash.get(colName + "_" + tblName + "_" + dbid);

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
		return new NounMetadata(recommendations, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.VIZ_RECOMMENDATION);
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
