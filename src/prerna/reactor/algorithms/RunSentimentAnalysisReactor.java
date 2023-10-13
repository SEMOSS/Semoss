package prerna.reactor.algorithms;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RunSentimentAnalysisReactor extends AbstractRFrameReactor {

	/**
	 * User enters a string column and the reactor will run sentiment analysis on that column
	 * and also give an advanced (more meaningful) aggregate sentiment of a grouping
	 * User can also add miscellaneous emotion columns if desired
	 */
	
	private static final String SENTIMENT_COLUMN_KEY = "sentimentCol";
	private static final String GROUP_COLUMN_KEY = "groupCol";
	private static final String EMOTIONS_KEY = "addEmotionCols";
	
	protected static final String CLASS_NAME = RunSentimentAnalysisReactor.class.getName();
		
	public RunSentimentAnalysisReactor() {
		this.keysToGet = new String[] { SENTIMENT_COLUMN_KEY, GROUP_COLUMN_KEY , EMOTIONS_KEY , ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get inputs
		init();
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		StringBuilder rsb = new StringBuilder();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String sentimentCol = this.keyValue.get(this.keysToGet[0]);
		String groupCol = this.keyValue.get(this.keysToGet[1]);
		Boolean addEmotionCols = addEmotionCols();
		RDataTable dataFrame = (RDataTable) getFrame();
		String frameName = dataFrame.getName();
		List<String> colHeaders = Arrays.asList( dataFrame.getColumnHeaders());
		boolean hasAgg = true;
		String sentimentFrame = "sentimentFrame" + Utility.getRandomString(5);
		
		// determine if we have an aggregate column
		if(groupCol == null || groupCol.isEmpty()) {
			hasAgg = false;
		}
		
		logger.info("Calculating column sentiment...");

		// source the files
		rsb.append("source(\"" + baseFolder.replace("\\", "/") + "/R/AnalyticsRoutineScripts/sentiment.R\");");
		
		// check if packages are installed
		String[] packages = { "sentimentr" };
		this.rJavaTranslator.checkPackages(packages);

		// let's first create the input frame
		// make sure to minimize number of rows with unique
		if (!hasAgg) {
			rsb.append(sentimentFrame + " <- data.frame(" + sentimentCol + " = " + frameName + "$" + sentimentCol + ");");
		} else {
			rsb.append(sentimentFrame + " <- data.frame(" + groupCol + " = " + frameName + "$" + groupCol + " " + " , "
					+ sentimentCol + " = " + frameName + "$" + sentimentCol + ");");
		}
		
		rsb.append(sentimentFrame + " <- unique(" + sentimentFrame + ");");
		
		// lets run the function
		rsb.append(sentimentFrame + " <- " + "score_sentiment(" + sentimentFrame);
		rsb.append(", review_col = \"" + sentimentCol + "\"");
		if(hasAgg) {
			rsb.append(", aggr_col = \"" + groupCol + "\"");
		} else {
			rsb.append(", aggr_col = NULL");
		}
		rsb.append(", emtn = " + addEmotionCols.toString().toUpperCase() + ");");
		
		// if this script returned an error, lets throw that error
		String isError = "sentimentError" + Utility.getRandomString(5);
		rsb.append("if(nrow(" + sentimentFrame + ") > 0 ) {" + isError + "<- \"\";}\n");	
		
		// run the rsb and get the column headers in the result
		this.rJavaTranslator.runR(rsb.toString());
		rsb.setLength(0);
		
		Boolean errorCheck = this.rJavaTranslator.getBoolean("!exists(\"" + isError + "\")");
		if(errorCheck) {
			throw new IllegalArgumentException("Sentiment could not be calculated");
		}
		
		// remove the columns from the current frame if they are about to be added
		// to avoid duplicates
		String[] newColHeaders = this.rJavaTranslator.getColumns(sentimentFrame);		
		removeDuplicateColumns(frameName,colHeaders,newColHeaders);
		
		// merge this back to frame based on input cols
		rsb.append("colnames("+sentimentFrame+")[colnames("+sentimentFrame+")==\""+ sentimentCol+"_of_" + sentimentCol +"\"] <- \""+sentimentCol+"\";");
		if(!hasAgg) {
			rsb.append(frameName + " <- merge(" + frameName + "," + sentimentFrame + ",by=c(\""+ sentimentCol +"\"));");
		} else {
			rsb.append("colnames("+sentimentFrame+")[colnames("+sentimentFrame+")==\""+ groupCol+"_of_" + sentimentCol +"\"] <- \""+groupCol+"\";");
			rsb.append(frameName + " <- merge(" + frameName + "," + sentimentFrame + ",by=c(\""+ groupCol +"\",\"" + sentimentCol + "\"));");
		}
		
		// convert to data table
		rsb.append(RSyntaxHelper.asDataTable(frameName, frameName));
		
		// run the R
		this.rJavaTranslator.runR(rsb.toString());
		
		// return the new frame
		RDataTable newTable = createNewFrameFromVariable(frameName);
		this.insight.setDataMaker(newTable);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(frameName, noun);
		
		// also return a success message and result
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Sentiment analysis ran successfully!"));	
		
		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + sentimentFrame + "," + isError + "); gc();");
		
		// return all
		return noun;
		
	}

	private void removeDuplicateColumns(String frameName, List<String> currentColHeaders, String[] newColHeaders) {
		List<String> colsToRemove = new Vector<String>();
		for(String newCol : newColHeaders) {
			if(currentColHeaders.contains(newCol)) {
				colsToRemove.add(newCol);
			}
		}
		if(!colsToRemove.isEmpty()) {
			// get the columns in needed string
			StringBuilder str = new StringBuilder("c(");
			for(int i = 0; i < colsToRemove.size(); i++) {
					str.append(colsToRemove.get(i));
				// if not the last entry, append a "," to separate entries
				if( (i+1) != colsToRemove.size()) {
					str.append(",");
				}
			}
			str.append(")");
			
			String script = frameName + "<- subset(" + frameName + ", select=-" + str.toString() + ");";
			this.rJavaTranslator.runR(script);
		}
	}

	/**
	 * Determine if we should override existing values
	 * @return
	 */
	
	private boolean addEmotionCols() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		if (grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}
	
///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SENTIMENT_COLUMN_KEY)) {
			return "The text column to run the sentiment analysis on";
		} else if (key.equals(GROUP_COLUMN_KEY)) {
			return "The column to group the sentiment analysis results on to show the aggregate results";
		} else if (key.equals(EMOTIONS_KEY)) {
			return "Boolean to determine whether or not to also determine the emotions behind the sentiment column text";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}