package prerna.reactor.federation;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class FuzzyMatchesReactor extends AbstractRFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(FuzzyMatchesReactor.class);

	private static final String CLASS_NAME = FuzzyMatchesReactor.class.getName();

	public static final String OUTPUT_FRAME_NAME = "outputFrame";
	public static final String FRAME_COLUMN = "frameCol";	

	private Logger logger = null;

	public FuzzyMatchesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey(), ReactorKeysEnum.FRAME.getKey(), FRAME_COLUMN, OUTPUT_FRAME_NAME};
	}

	@Override
	public NounMetadata execute() {

		/*
		 * The logic for this is to get 2 data.tables with 1 column each
		 * and then run them through the best_match method. 
		 * 
		 * The best_match returns a new table of col1, col2, distance where 
		 * col1 is the values in the first data.table, 
		 * col2 is the values in the second data.table, 
		 * and distance is the measure of closeness between these values. 
		 * This table compares every value in the first data.table to the values
		 * in the second data.table.
		 * 
		 * The majority of the logic is in getting the 2 data.tables since one comes 
		 * from a task (or a QS that we flush into a task) and the other can come from a similar fashion
		 * or can come from a frame + frame_column that is passed into reactor.  To further optimize, if the frame
		 * is an R frame, we run R code to get the second data.table instead of running a frame query,
		 * flushing to a TSV, and then reading in R
		 * 
		 */

		init();
		this.logger = getLogger(CLASS_NAME);

		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		this.rJavaTranslator.checkPackages(packages);

		// the main script to execute
		StringBuilder script = new StringBuilder();
		script.append("library(data.table);library(stringdist);");

		// string of table to return
		final String matchesFrame = getOutputFrame();
		final String rCol1 = matchesFrame + "col1";
		final String rCol2 = matchesFrame + "col2";

		List<File> cleanUpFiles = new Vector<File>();

		// flush the first results into rcol1
		{
			logger.info("Creating first vector of values to compare");
			ITask it1 = getTask(0);
			String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";
			File newFile = Utility.writeResultToFile(newFileLoc, it1, null, "\t");
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(rCol1, newFile.getAbsolutePath(), "\t");
			script.append(loadFileRScript);
			cleanUpFiles.add(newFile);
		}

		// flush the second results into rcol2
		// but if we already have an R variable
		// we will optimize
		logger.info("Creating second vector of values to compare");
		boolean optimized = false;
		String frameCol = getFrameColumn();
		if(frameCol.contains("__")) {
			frameCol = frameCol.split("__")[1];
		}
		if(frameCol != null) {
			ITableDataFrame frame = getFrame();
			if(frame instanceof RDataTable) {
				optimized = true;
				// optimize for R frame
				String getColScript = rCol2 + " <- as.character(" + frame.getName() + "$" + frameCol + ");";
				script.append(getColScript);
			} else {
				// create a task from the frame + frame col
				// write to TSV
				// read in R
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.addSelector(new QueryColumnSelector(frameCol));
				
				File newFile = null;
				IRawSelectWrapper iterator = null;
				try {
					iterator = frame.query(qs);
					ITask it2 = new BasicIteratorTask(qs, iterator);
					String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";
					newFile = Utility.writeResultToFile(newFileLoc, it2, null, "\t");
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new SemossPixelException(e.getMessage());
				} finally {
					if(iterator != null) {
						try {
							iterator.close();
						} catch (IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
				String loadFileRScript = RSyntaxHelper.getFReadSyntax(rCol2, newFile.getAbsolutePath(), "\t");
				script.append(loadFileRScript);
				cleanUpFiles.add(newFile);
			}
		} else {
			// another task has been passed directly into the reactor
			// grab it and flush into a TSV
			// read in R
			ITask it2 = getTask(1);
			String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";
			File newFile = Utility.writeResultToFile(newFileLoc, it2, null, "\t");
			String loadFileRScript = RSyntaxHelper.getFReadSyntax(rCol2, newFile.getAbsolutePath(), "\t");
			script.append(loadFileRScript);
			cleanUpFiles.add(newFile);
		}

		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		// source the script
		script.append("source(\"" + baseFolder.replace("\\", "/") + "/R/Recommendations/advanced_federation_blend.r\");");
		// create the matches frame using the best_match method
		if(optimized) {
			script.append(matchesFrame + " <- best_match(" + rCol1 + "[[names(" + rCol1 + ")[1]]]," + rCol2 + ");");
		} else {
			script.append(matchesFrame + " <- best_match(" + rCol1 + "[[names(" + rCol1 + ")[1]]]," + rCol2 + "[[names(" + rCol2 + ")[1]]]);");
		}
		// add a unique combined col1 == col2, remove extra columns
		script.append(matchesFrame + "$distance <- as.numeric(" + matchesFrame + "$dist);");
		script.append(matchesFrame + "<-" + matchesFrame + "[,c(\"col1\",\"col2\",\"distance\")];");
		script.append(matchesFrame + "<-" + matchesFrame + "[order(unique(" + matchesFrame + ")$distance),];");
		// convert col1/col2 from factor to list
		script.append(matchesFrame+"$col1<-as.character("+matchesFrame+"$col1);");
		script.append(matchesFrame+"$col2<-as.character("+matchesFrame+"$col2);");
		script.append("rm(" + rCol1 + "," + rCol2 + ");");

		logger.info("Running script to generate all fuzzy matches");
		this.rJavaTranslator.runR(script.toString());
						
		RDataTable returnTable = null;
		NounMetadata retNoun = null;
		// get count of exact matches and check if matches are found
		String exactMatchCount = this.rJavaTranslator.getString("as.character(nrow(" + matchesFrame + "[" + matchesFrame + "$distance == 0,]))");
		if (exactMatchCount != null) {
			int val = Integer.parseInt(exactMatchCount);
			returnTable = createNewFrameFromVariable(matchesFrame);
			retNoun = new NounMetadata(returnTable, PixelDataType.FRAME, PixelOperationType.FRAME);
			retNoun.addAdditionalReturn(new NounMetadata(val, PixelDataType.CONST_INT));
		} else{
			throw new IllegalArgumentException("No matches found.");
		}

		// clean up files
		for(File f : cleanUpFiles) {
			f.delete();
		}

		// add to the insight store
		this.insight.getVarStore().put(matchesFrame, retNoun);
		return retNoun;
	}

	////////////////////////////////////////////////////////////

	/*
	 * Getting the inputs 
	 */

	/**
	 * Get the task to use
	 * @return
	 */
	private ITask getTask(int index) {
		// will check for a proper Task or a QS
		ITask task = null;

		GenRowStruct grsTasks = this.store.getNoun(PixelDataType.TASK.getKey());
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(grsTasks != null && grsTasks.size() > index) {
			task = (ITask) grsTasks.get(index);
		} else {
			List<Object> tasks = this.curRow.getValuesOfType(PixelDataType.TASK);
			if(tasks != null && tasks.size() > index) {
				task = (ITask) tasks.get(index);
			}
		}

		if(task == null) {
			task = constructTaskFromQs(index);
		}
		task.setLogger(this.logger);
		return task;
	}

	/**
	 * Generate the task from the query struct
	 * @return
	 */
	private ITask constructTaskFromQs(int index) {
		NounMetadata noun = null;
		SelectQueryStruct qs = null;

		GenRowStruct grsQs = this.store.getNoun(PixelDataType.QUERY_STRUCT.getKey());
		//if we don't have tasks in the curRow, check if it exists in genrow under the qs key
		if(grsQs != null && grsQs.size() > index) {
			noun = grsQs.getNoun(index);
			qs = (SelectQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if(qsList != null && qsList.size() > index) {
				noun = qsList.get(index);
				qs = (SelectQueryStruct) noun.getValue();
			}
		}

		// no qs either... i guess we will return an empty constant data task
		// this will just store the information
		if(qs == null) {
			throw new IllegalArgumentException("No data found to fuzzy match");
		}

		// handle some defaults
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		// first, do a basic check
		if(qsType != QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY && qsType != QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			// it is not a hard query
			// we need to make sure there is at least a selector
			if(qs.getSelectors().isEmpty()) {
				throw new IllegalArgumentException("There are no selectors in the query to return.  "
						+ "There must be at least one selector for the query to execute.");
			}
		}

		if(qsType == QUERY_STRUCT_TYPE.FRAME || qsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			ITableDataFrame frame = qs.getFrame();
			if(frame == null) {
				frame = (ITableDataFrame) this.insight.getDataMaker();
			}
			qs.setFrame(frame);
			qs.mergeImplicitFilters(frame.getFrameFilters());
		}

		ITask task = new BasicIteratorTask(qs);
		// add the task to the store
		this.insight.getTaskStore().addTask(task);
		return task;
	}

	/**
	 * Get the frame column to use
	 * @return String containing the frame column header
	 */
	private String getFrameColumn() {
		GenRowStruct grs = this.store.getNoun(FRAME_COLUMN);
		if(grs != null && !grs.isEmpty()) {
			String frameCol = grs.get(0).toString().trim();
			if(!frameCol.isEmpty()) {
				return frameCol;
			}
		}
		return null;
	}

	/**
	 * Get the name of the frame to output
	 * @return
	 */
	private String getOutputFrame() {
		GenRowStruct grs = this.store.getNoun(OUTPUT_FRAME_NAME);
		if(grs != null && !grs.isEmpty()) {
			String outFrame = grs.get(0).toString().trim();
			if(!outFrame.isEmpty()) {
				return outFrame;
			}
		}
		return "fuzzyFrame_" + Utility.getRandomString(6);
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FRAME_COLUMN)) {
			return "The column from the frame to join on";
		} else if(key.equals(OUTPUT_FRAME_NAME)){
			return "Specify the output frame name";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
