package prerna.reactor.algorithms;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GetNLPVizOptionsReactor extends AbstractRFrameReactor {

	/**
	 * Reads in the Columns and Database IDs and returns the visualization string, which
	 * is then appended to the NLP search
	 */
	
	protected static final String SORT_PIXEL = "sortPixel";
	protected static final String CLASS_NAME = CreateNLPVizReactor.class.getName();

	public GetNLPVizOptionsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.COLUMNS.getKey() , ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		List<String> cols = getColumns();
		RDataTable frame = (RDataTable) this.getFrame();
		String frameName = frame.getName();
		StringBuilder rsb = new StringBuilder();
		Map<String, String> aliasHash = new HashMap<String, String>();
		String fileroot = "dataitem";
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		String databaseName = "Multiple";
		if(!databaseId.contains("Multiple") && !databaseId.contains("null")) {
			databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseId).replace(" ", "_");
		}
		boolean allStrings = true;
		
		// set the intial list options -- only Recommended and Grid
		String[] gridArray = {"Recommended","Grid"};
		
		// if it only has one column or one row, just return it as a grid
		if (cols.size() < 2 || frame.getNumRows(frameName) < 2) {
			return new NounMetadata(gridArray, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}

		// check if packages are installed
		String[] packages = { "data.table", "plyr" , "jsonlite" };
		this.rJavaTranslator.checkPackages(packages);
		
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

			// get datatype
			String dataType = metadata.getHeaderTypeAsString(tableName + "__" + colName);
			
			// If it is an int or double, convert to NUMBER
			if(dataType.equalsIgnoreCase("INT") || dataType.equalsIgnoreCase("DOUBLE")) {
				dataType = "NUMBER";
			}
			
			// check to make sure at least one column is not a string
			if(!dataType.equalsIgnoreCase("STRING")) {
				allStrings = false;
			}

			// get unique column values -- if it is an aggregate, then make it 30
			int uniqueValues = 0;
			if (isAggregate(col)) {
				uniqueValues = 30;
			} else {
				uniqueValues = frame.getUniqueInstanceCount(colName);
			}
			

			rsb.append(inputFrame + "[" + rowCounter + ",] <- c(\"" + databaseId + "$" + databaseName + "$" + tableName + "$" + colName + "\",");
			rsb.append("\"" + dataType + "\",");
			rsb.append(uniqueValues + ");");

			// add column alias hash so we can look up alias later
			aliasHash.put(colName + "_" + tableName + "_" + databaseId, col.toString());

			rowCounter++;
		}
		
		// if all the columns were strings, then make it a grid
		if(allStrings) {
			return new NounMetadata(gridArray, PixelDataType.CUSTOM_DATA_STRUCTURE);
		}

		// now let's look for the shared history vs personal history
		File userHistory = new File(baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "Recommendations" + DIR_SEPARATOR + "dataitem-user-history.rds");
		if (!userHistory.exists()) {
			// user history does not exist, let's use shared history
			fileroot = "shared";
		}		

		// now lets run the script and return an array
		// source the files and init
		String wd = "wd" + Utility.getRandomString(5);
		rsb.append(wd + " <- getwd();");
		rsb.append("setwd(\"" + baseFolder.replace("\\", "/") + "/R/Recommendations/\");");
		rsb.append("source(\"viz_selection.r\");");
		rsb.append("source(\"viz_recom.r\");");

		
		// change int/double to number in the history
		rsb.append("sync_numeric(\"" +fileroot + "\",\"" + fileroot + "\");");		

		// run function
		String output = "output" + Utility.getRandomString(5);
		rsb.append(output + "<- character(0);");
		rsb.append(output + " <- get_viz_choices(\"" + fileroot + "\"," + inputFrame + ");");
		rsb.append(output + " <- c(\"Recommended\"," + output + ",\"Grid\")");
		this.rJavaTranslator.runR(rsb.toString());
		
		// get the list
		String[] retArray = this.rJavaTranslator.getStringArray(output);
		
		// garbage clean up in R
		this.rJavaTranslator.executeEmptyR("setwd(" + wd + ");");
		this.rJavaTranslator.executeEmptyR("rm(" + wd + "," + output + "," + inputFrame + "); gc();");

		// Return Array
		return new NounMetadata(retArray, PixelDataType.CUSTOM_DATA_STRUCTURE);
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
}