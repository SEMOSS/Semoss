package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

// CreateNLPViz(app=["2c8c41da-391a-4aa8-a170-9925211869c8"],columns=[Studio, Average_MovieBudget],frame=["FRAME_ak28Db"])

public class CreateNLPVizReactor extends AbstractRFrameReactor {

	/**
	 * Reads in the Columns and App IDs and returns the visualization string, which
	 * is then appended to the NLP search
	 */

	protected static final String CLASS_NAME = CreateNLPVizReactor.class.getName();

	public CreateNLPVizReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.COLUMNS.getKey(),
				ReactorKeysEnum.FRAME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String appId = this.keyValue.get(this.keysToGet[0]);
		List<String> cols = getColumns();
		RDataTable frame = (RDataTable) this.getFrame();
		String frameName = frame.getName();
		StringBuilder rsb = new StringBuilder();
		Map<String, String> aliasHash = new HashMap<String, String>();
		String fileroot = "dataitem";
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		String appName = MasterDatabaseUtility.getEngineAliasForId(appId).replace(" ", "_");

		// check if packages are installed
		String[] packages = { "data.table", "plyr" , "jsonlite" };
		this.rJavaTranslator.checkPackages(packages);

		// let's first create the input frame
		String inputFrame = "inputFrame" + Utility.getRandomString(5);
		rsb.append(inputFrame + " <- data.frame(reference1 = character(), reference2 = character(), reference3 = integer(), stringsAsFactors = FALSE);");
		int rowCounter = 1;
		for (String col : cols) {
			String tableName = null;
			String colName = null;

			// lets get the table name, column name, and type (if possible)
			if (col.contains("__")) {
				// this both a table and a column
				String[] parsedCol = col.split("__");
				tableName = parsedCol[0];
				colName = parsedCol[1];
			} else {
				// this is a table and column (primary key)
				// or an aggregate
				tableName = frameName;
				colName = col;
			}

			String dataType = metadata.getHeaderTypeAsString(tableName + "__" + colName);

			// get unique column values
			int uniqueValues = frame.getUniqueInstanceCount(colName);

			rsb.append(inputFrame + "[" + rowCounter + ",] <- c(\"" + appId + "$" + appName + "$" + tableName + "$" + colName + "\",");
			rsb.append("\"" + dataType + "\",");
			rsb.append(uniqueValues + ");");

			// add column alias hash so we can look up alias later
			aliasHash.put(colName + "_" + tableName + "_" + appId, col.toString());

			rowCounter++;
		}

		// now let's look for the shared history vs personal history
		File userHistory = new File(baseFolder + "\\R\\Recommendations\\dataitem-user-history.rds");
		if (!userHistory.exists()) {
			// user history does not exist, let's use shared history
			fileroot = "shared";
			logger.info("Selecting proper visualization for insight");
		}

		// now lets run the script and return a json
		String outputJson = "outputJson" + Utility.getRandomString(5);
		String recommend = "recommend" + Utility.getRandomString(5);
		rsb.append("origDir <- getwd();");
		rsb.append("setwd(\"" + baseFolder.replace("\\", "/") + "/R/Recommendations/\");");
		rsb.append("source(\"viz_recom.r\");");
		rsb.append(recommend + "<-viz_recom_mgr(\"" + fileroot + "\", " + inputFrame + ", \"Grid\", 1);");
		rsb.append("library(jsonlite);");
		rsb.append(outputJson + " <-toJSON(" + recommend + ", byrow = TRUE, colNames = TRUE);");
		rsb.append("setwd(origDir);");
		this.rJavaTranslator.runR(rsb.toString());

		// receive json string from R
		String json = this.rJavaTranslator.getString(outputJson + ";");
		Map recommendations = new HashMap<String, HashMap<String, String>>();

		// if no recommendation was found, lets just return it in a grid
		if (json == null) {
			String nullPixel = "Frame () | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 );";
			PixelRunner runner = this.insight.runPixel(nullPixel);
			return runner.getResults().get(0);
//			Map<String, Object> runnerWraper = new HashMap<String, Object>();
//			runnerWraper.put("runner", runner);
//			return new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.VECTOR);
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
					map.put(component, columnAlias);
					recommendations.put(chart, map);
				} else {
					map.put(component, columnAlias);
					map.put("tooltip", "");
					recommendations.put(chart, map);
				}
			}
		}

		// garbage clean up in R
		this.rJavaTranslator.executeEmptyR(
				"rm( " + "get_reference," + "inputFramea6JSRk," + "origDir," + "outputJsonaDbXom," + "recommenda5udnq,"
						+ "restore_datatype" + "viz_history," + "viz_recom," + "viz_recom_mgr" + " ); gc();");

		String returnPixel = generatePixelFromRec(recommendations, cols, frameName);
		PixelRunner runner = this.insight.runPixel(returnPixel);
		return runner.getResults().get(0);
//		Map<String, Object> runnerWraper = new HashMap<String, Object>();
//		runnerWraper.put("runner", runner);
//		return new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.VECTOR);
	}

	private List<String> getColumns() {
		List<String> engineFilters = new Vector<String>();
		GenRowStruct engineGrs = this.store.getNoun(this.keysToGet[1]);
		for (int i = 0; i < engineGrs.size(); i++) {
			engineFilters.add(engineGrs.get(i).toString());
		}

		return engineFilters;
	}

	private String generatePixelFromRec(Map<String, HashMap<String, String>> recommendations, List<String> columns,
			String frameName) {
		StringBuilder pixel = new StringBuilder();
		String chartType = recommendations.keySet().toArray()[0].toString();

		pixel.append("Frame ( frame = [ " + frameName + " ] ) | ");

		// Append the Select and the Alias
		// currently left separate in case changes need to be made in future
		String selectString = "Select ( ";
		String aliasString = ".as ( [ ";
		String delim = "";
		for (String col : columns) {
			// handle comma issue
			selectString += delim;
			aliasString += delim;
			delim = " , ";

			// add to the select and alias strings
			if (col.contains("__")) {
				col = col.split("__")[1];
			}
			selectString += Utility.cleanVariableString(col);

			// clean the alias string then append also
			if (col.contains("__")) {
				col = col.split("__")[1];
			}
			aliasString += Utility.cleanVariableString(col);
		}
		pixel.append(selectString + " ) ").append(aliasString + " ] ) | ");

		// Now add some formatting
		pixel.append("With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | ");

		// Now add the task options
		pixel.append("TaskOptions ( { \"0\" : { \"layout\" : \"" + chartType + "\" , \"alignment\" : {");

		// Now add the details from recommendations map
		delim = "";
		for (Map.Entry<String, String> entry : recommendations.get(chartType).entrySet()) {
			// handle comma issue
			pixel.append(delim);
			delim = " , ";
			String value = entry.getValue();
			if (value.contains("__")) {
				value = value.split("__")[1];
			}
			selectString += Utility.cleanVariableString(value);
			pixel.append("\"" + entry.getKey() + "\" : [ \"" + value + "\" ]");
		}

		// wrap it up and return
		pixel.append("} } } ) | Collect ( 2000 );");
		return pixel.toString();
	}
}