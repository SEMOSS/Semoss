package prerna.reactor.algorithms;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CreateNLPVizReactor extends AbstractRFrameReactor {

	/**
	 * Reads in the Columns and Database IDs and returns the visualization string, which
	 * is then appended to the NLP search
	 */

	protected static final String SORT_PIXEL = "sortPixel";
	protected static final String VIZ_SELECTION = "vizSelection";
	protected static final String CLASS_NAME = CreateNLPVizReactor.class.getName();

	public CreateNLPVizReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.COLUMNS.getKey(), SORT_PIXEL,
				ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.PANEL.getKey(), VIZ_SELECTION };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		String panelId = getPanelId();
		List<String> cols = getColumns();
		String sortPixel = this.keyValue.get(this.keysToGet[2]);
		String vizSelection = this.keyValue.get(this.keysToGet[5]);
		RDataTable frame = (RDataTable) this.getFrame();
		String frameName = frame.getName();
		StringBuilder rsb = new StringBuilder();
		Map<String, String> aliasHash = new HashMap<String, String>();
		String fileroot = "dataitem";
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		String databaseName = "Multiple";
		if (!databaseId.contains("Multiple") && !databaseId.contains("null")) {
			databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId).replace(" ", "_");
		}
		boolean allStrings = true;
		
		// check if packages are installed
		String[] packages = { "data.table", "plyr", "jsonlite" };
		this.rJavaTranslator.checkPackages(packages);
		
		// have pixel to paint as grid
		String gridPixel = "Frame () | QueryAll ( ) | " + sortPixel + " AutoTaskOptions ( panel = [ \"" + panelId
				+ "\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 );";

		// if user wants 'Grid' then visualize as grid
		if (vizSelection.equals("Grid")) {
			PixelRunner runner = this.insight.runPixel(gridPixel);
			return runner.getResults().get(0);
		}

		// sort the columns list in ascending order of unique inst
		// put the aggregate columns last
		Collections.sort(cols, new Comparator<String>() {
			public int compare(String firstCol, String secondCol) {
				if (isAggregate(firstCol)) {
					return 1;
				} else {
					return frame.getUniqueInstanceCount(firstCol) - frame.getUniqueInstanceCount(secondCol);
				}
			}
		});

		// let's first create the input frame
		String inputFrame = "inputFrame" + Utility.getRandomString(5);
		rsb.append(inputFrame
				+ " <- data.frame(reference1 = character(), reference2 = character(), reference3 = integer(), stringsAsFactors = FALSE);");
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

			// get datatype
			String dataType = metadata.getHeaderTypeAsString(tableName + "__" + colName);

			// If it is an int or double, convert to NUMBER
			if (dataType.equalsIgnoreCase("INT") || dataType.equalsIgnoreCase("DOUBLE")) {
				dataType = "NUMBER";
			}
			
			// catch the date functions and flip it to string if so
			String[] dates = { "DAYNAME", "MONTHNAME", "WEEK", "QUARTER", "YEAR" };
			if(Stream.of(dates).anyMatch(colName.toUpperCase()::startsWith)) {
				dataType = "STRING";
			}

			// check to make sure at least one column is not a string
			if (!dataType.equalsIgnoreCase("STRING")) {
				allStrings = false;
			}

			// get unique column values -- if it is an aggregate, then make it 30
			int uniqueValues = 0;
			if (isAggregate(col)) {
				uniqueValues = 30;
			} else {
				uniqueValues = frame.getUniqueInstanceCount(colName);
			}

			rsb.append(inputFrame + "[" + rowCounter + ",] <- c(\"" + databaseId + "$" + databaseName + "$" + tableName + "$"
					+ colName + "\",");
			rsb.append("\"" + dataType + "\",");
			rsb.append(uniqueValues + ");");

			// add column alias hash so we can look up alias later
			aliasHash.put(colName + "_" + tableName + "_" + databaseId, col.toString());

			rowCounter++;
		}

		// if user wants 'Recommended' then call function to use usual flow
		// otherwise, use new flow
		// now let's look for the shared history vs personal history
		File userHistory = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations"
				+ DIR_SEPARATOR + "dataitem-user-history.rds");
		if (!userHistory.exists()) {
			// user history does not exist, let's use shared history
			fileroot = "shared";
			logger.info("Selecting proper visualization for insight");
		}
		
		// now lets run the script and return a json
		String json = "";
		String outputJson = "outputJson" + Utility.getRandomString(5);
		String recommend = "recommend" + Utility.getRandomString(5);
		String wd = "wd" + Utility.getRandomString(5);
		rsb.append(wd + " <- getwd();");
		rsb.append("setwd(\"" + baseFolder.replace("\\", "/") + "/R/Recommendations/\");");
		rsb.append("source(\"viz_recom.r\");");
	
		// change int/double to number in the history
		rsb.append("sync_numeric(\"" + fileroot + "\",\"" + fileroot + "\");");
					
		if (vizSelection.equals("Recommended")) {
			// if it only has one column or one row, just return it as a grid
			if (cols.size() < 2 || frame.getNumRows(frameName) < 2 || allStrings) {
				PixelRunner runner = this.insight.runPixel(gridPixel);
				return runner.getResults().get(0);
			}

			// run function and create json
			rsb.append(recommend + "<-viz_recom_mgr(\"" + fileroot + "\", " + inputFrame + ", \"Grid\", 1);");
			rsb.append("library(jsonlite);");
			rsb.append(outputJson + " <-toJSON(" + recommend + ", byrow = TRUE, colNames = TRUE);");
			rsb.append("setwd(" + wd + ");");
			this.rJavaTranslator.runR(rsb.toString());

			// receive json string from R
			json = this.rJavaTranslator.getString(outputJson + ";");

		} else {
			rsb.append("source(\"viz_selection.r\");");

			// run function and create json
			rsb.append(recommend + " <- get_viz_choices(\"" + fileroot + "\"," + inputFrame + ",\"" + vizSelection + "\");");
			rsb.append("library(jsonlite);");
			rsb.append(outputJson + " <-toJSON(" + recommend + ", byrow = TRUE, colNames = TRUE);");
			rsb.append("setwd(" + wd + ");");
			this.rJavaTranslator.runR(rsb.toString());

			// receive json string from R
			json = this.rJavaTranslator.getString(outputJson + ";");
		}

		Map recommendations = new HashMap<String, HashMap<String, String>>();

		// if no recommendation was found, lets just return it in a grid
		if (json == null || json.equals("[]")) {
			PixelRunner runner = this.insight.runPixel(gridPixel);
			return runner.getResults().get(0);
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
		
		//garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + wd + "," + outputJson + "," + inputFrame + "," + recommend + "); gc();");

		// return
		String returnPixel = generatePixelFromRec(recommendations, cols, frameName, metadata, panelId, sortPixel);
		PixelRunner runner = this.insight.runPixel(returnPixel);
		return runner.getResults().get(0);
	}

	private boolean isAggregate(String col) {
		return (col.contains("UniqueCount_") || col.contains("Count_") || col.contains("Min_") || col.contains("Max_")
				|| col.contains("Average_") || col.contains("Sum_"));
	}

	private List<String> getColumns() {
		List<String> columns = new Vector<String>();
		GenRowStruct columnGRS = this.store.getNoun(this.keysToGet[1]);
		for (int i = 0; i < columnGRS.size(); i++) {
			columns.add(columnGRS.get(i).toString());
		}

		return columns;
	}

	private String generatePixelFromRec(Map<String, HashMap<String, String>> recommendations, List<String> columns,
			String frameName, OwlTemporalEngineMeta metadata, String panelId, String sortPixel) {
		StringBuilder pixel = new StringBuilder();
		String chartType = recommendations.keySet().toArray()[0].toString();

		// get the label to ensure that it is not averaged if number
		String label = "";
		for (Map.Entry<String, String> entry : recommendations.get(chartType).entrySet()) {
			if (entry.getKey().equals("label")) {
				label = entry.getValue();
			}
		}

		// prep to catch the error
		pixel.append("ifError( ( ");

		pixel.append("Frame ( frame = [ " + frameName + " ] ) | ");

		// Append the Select and the Alias
		// currently left separate in case changes need to be made in future
		String selectString = "Select ( ";
		String aliasString = ".as ( [ ";
		String delim = "";
		List<String> groupedCols = new ArrayList<String>();
		for (String col : columns) {
			// check data type to see if num and store grouped column if string and not
			// label
			String dataType = metadata.getHeaderTypeAsString(frameName + "__" + col);

			// handle comma issue
			selectString += delim;
			aliasString += delim;
			delim = " , ";

			// add to the select and alias strings
			// if it is a number, default group it as an average if not label
			if (col.contains("__")) {
				col = col.split("__")[1];
			}
			if (!label.equals(col) && (Utility.isIntegerType(dataType) || Utility.isDoubleType(dataType))) {
				selectString += "Average ( " + Utility.cleanVariableString(col) + " )";
			} else {
				selectString += Utility.cleanVariableString(col);
				groupedCols.add(Utility.cleanVariableString(col));
			}

			// clean the alias string then append also
			if (col.contains("__")) {
				col = col.split("__")[1];
			}
			aliasString += Utility.cleanVariableString(col);
		}
		pixel.append(selectString + " ) ").append(aliasString + " ] ) | ");

		// add the grouping in the case where we had to make an average
		pixel.append("Group( ");
		delim = "";
		for (String col : groupedCols) {
			pixel.append(delim);
			delim = " , ";
			pixel.append(col);
		}
		pixel.append(") | ");

		// add the sort if any
		pixel.append(sortPixel);

		// Now add some formatting
		pixel.append("With ( Panel ( " + panelId + " ) ) | Format ( type = [ 'table' ] ) | ");

		// Now add the task options
		pixel.append("TaskOptions ( { \"" + panelId + "\" : { \"layout\" : \"" + chartType + "\" , \"alignment\" : {");

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
		pixel.append("} } } ) | Collect ( 2000 )");

		// catch error and paint as grid
		pixel.append(") , ( Frame () | QueryAll ( ) | " + sortPixel + "AutoTaskOptions ( panel = [ \"" + panelId
				+ "\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ) );");

		return pixel.toString();
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return "0";
	}
}