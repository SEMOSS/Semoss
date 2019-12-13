package prerna.sablecc2.reactor.frame.r;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ImputeNullValuesReactor extends AbstractRFrameReactor {

	/**
	 * This reactor imputes the value of null values in numeric columns to fill in
	 * null values with best guesses
	 */
	
	//  ImputeNullValues ( column = ["bp_2d"] , imputeCols = [ "bp_1d" , "bp_1s" , "bp_2s" ] , crossSectionCol = [ "frame" ] ) ;

	protected static final String CLASS_NAME = ImputeNullValuesReactor.class.getName();
	private static final String CROSS_SECTION_COL = "crossSectionCol";
	private static final String IMPUTE_COLS = "imputeCols";

	public ImputeNullValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey() , IMPUTE_COLS, CROSS_SECTION_COL };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		int stepCounter = 1;
		Logger logger = this.getLogger(CLASS_NAME);
		StringBuilder rsb = new StringBuilder();
		String imputedFrame = "imputed" + Utility.getRandomString(5);
		String retFrame = "result" + Utility.getRandomString(5);

		// Check Packages
		logger.info(stepCounter + ". Checking R Packages");
		String[] packages = new String[] { "Amelia" };
		this.rJavaTranslator.checkPackages(packages);
		rsb.append("library(Amelia);");
		
		// get the temp file path to store the imputed files
		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePath = filePath.replace("\\", "/");

		// change the working directory
		String wd = "wd" + Utility.getRandomString(5);
		rsb.append(wd + "<- getwd();");
		rsb.append("setwd(\"" + filePath + "\");");

		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String table = frame.getName();

		// get inputs
		String updateColumn = getUpdateColumn();
		List<String> updateCols = getColumns();
		String crossSectionCol = getCrossSectionColumn();

		// filter to only the chosen columns (cbind this back to the frame later)
		rsb.append(imputedFrame + " <- " + getSubsetFrameString(table, updateCols, updateColumn, crossSectionCol));

		// remove the chosen columns from the base frame to merge back later
		rsb.append(table + " <- subset(" + table + ", select = -c(" + updateColumn + "));");
		
		// create the imputed frame
		rsb.append(imputedFrame + " <- amelia(x=" + imputedFrame + ", cs=\"" + crossSectionCol + "\");");

		// write each imputed table to a csv to directory
		String fileName = "imputeddata";
		rsb.append(
				"write.amelia(" + imputedFrame + ",separate=TRUE,\"" + fileName + "\",extension=NULL,format=\"csv\");");

		// read in those files to get the result
		rsb.append(retFrame + " <- read.table(\"" + filePath + "/" + fileName + "5" + "\",header=TRUE,sep=\",\",na.strings=\"NA\",dec=\".\",strip.white=TRUE);");
		rsb.append(retFrame + " <- subset(" + retFrame + ", select = c(" + updateColumn + "));");
		
		// merge this frame back to the existing frame 
		rsb.append(table + " <- cbind(" + retFrame + "," + table + ");");
		
		// run the R
		frame.executeRScript(rsb.toString());
		
		// reset the working directory
		frame.executeRScript("setwd(" + wd + ");");
		
		// garbage cleanup
		frame.executeRScript("rm(" + imputedFrame + "," + retFrame +"); gc();");
		frame.executeRScript(RSyntaxHelper.asDataTable(table, table));

		// now override the current frame with this Frame and return
		RDataTable newTable = createNewFrameFromVariable(table);
		this.insight.setDataMaker(newTable);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(table, noun);
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ImputeNullValues", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// return
		return noun;
	}

	private Object getSubsetFrameString(String table, List<String> updateCols, String updateColumn, String crossSectionCol) {
		StringBuilder rsb = new StringBuilder();

		// start string with cross section col and table name
		rsb.append("subset(" + table + ", select = c(" + crossSectionCol + " , " + updateColumn);

		// add each numeric column
		for (String col : updateCols) {
			if(!col.equals(updateColumn) && !col.equals(crossSectionCol)) {
				rsb.append(", " + col);
			}
		}

		// close up
		rsb.append("));");

		// return
		return rsb.toString();
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	private String getUpdateColumn() {
		GenRowStruct inputsGRS = this.store.getNoun(this.keysToGet[0]);
		if (inputsGRS == null) {
			inputsGRS = this.getCurRow();
		}
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first noun will be the column to update
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullUpdateCol = noun1.getValue() + "";
			if (fullUpdateCol.contains("__")) {
				fullUpdateCol = fullUpdateCol.split("__")[1];
			}
			if (fullUpdateCol.length() == 0) {
				throw new IllegalArgumentException("Need to define column to update");
			}
			return fullUpdateCol;
		}
		throw new IllegalArgumentException("Need to define column to update");
	}
	
	private List<String> getColumns() {
		List<String> cols = new ArrayList<String>();

		// try its own key
		GenRowStruct colsGrs = this.store.getNoun(keysToGet[1]);
		if (colsGrs != null && !colsGrs.isEmpty()) {
			int size = colsGrs.size();
			for (int i = 0; i < size; i++) {
				cols.add(colsGrs.get(i).toString());
			}
			return cols;
		}

		int inputSize = this.getCurRow().size();
		if (inputSize > 0) {
			for (int i = 0; i < inputSize; i++) {
				cols.add(this.getCurRow().get(i).toString());
			}
			return cols;
		}

		throw new IllegalArgumentException("Need to define the columns to impute based on");
	}

	private String getCrossSectionColumn() {
		GenRowStruct inputsGRS = this.store.getNoun(this.keysToGet[2]);
		if (inputsGRS == null) {
			inputsGRS = this.getCurRow();
		}
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first noun will be the column to update
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullUpdateCol = noun1.getValue() + "";
			if (fullUpdateCol.contains("__")) {
				fullUpdateCol = fullUpdateCol.split("__")[1];
			}
			if (fullUpdateCol.length() == 0) {
				throw new IllegalArgumentException("Need to define cross section column");
			}
			return fullUpdateCol;
		}
		throw new IllegalArgumentException("Need to define cross section column");
	}
}
