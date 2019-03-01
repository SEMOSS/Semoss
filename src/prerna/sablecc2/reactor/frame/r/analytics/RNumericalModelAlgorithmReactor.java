package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.Utility;

public class RNumericalModelAlgorithmReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RNumericalModelAlgorithmReactor.class.getName();

	/**
	 * RunNumericalSimilarity(column = ["age"], panel=["0"]);
	 */

	public RNumericalModelAlgorithmReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String[] packages = new String[] { "data.table", "nueralnet" };
		this.rJavaTranslator.checkPackages(packages);
		Logger logger = this.getLogger(CLASS_NAME);
		RDataTable dataFrame = (RDataTable) getFrame();
		String frameName = dataFrame.getName();
		dataFrame.setLogger(logger);
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();

		// get inputs
		String instanceColumn = this.keyValue.get(this.keysToGet[0]);
		String panelId = this.keyValue.get(this.keysToGet[1]);

		// ensure that datatype of column is numeric
		if (!dataFrame.isNumeric(instanceColumn)) {
			// now return this object
			NounMetadata noun = new NounMetadata("Numerical Similarity can only be run on a numerical column",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			return noun;
		}

		// determine the name for the new similarity column to avoid adding
		// columns with same name
		String newColName = instanceColumn + "_Predicted";
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(dataFrame.getName(), newColName);

		// get the data from the numerical similarity algorithm
		logger.info("Start iterating through data to determine model");
		boolean success = runAlgorithm(frameName, instanceColumn, newColName);
		logger.info("Done iterating through data to determine model");

		if (success) {
			// create the new frame meta
			meta.addProperty(frameName, frameName + "__" + newColName);
			meta.setAliasToProperty(frameName + "__" + newColName, newColName);
			meta.setDataTypeToProperty(frameName + "__" + newColName, "DOUBLE");

			// now lets visualize the difference between actual and predicted
			// lets do a scatter

			// we need to add a unique row id
			String[] dataTableHeaders = new String[] { "ROW_ID", instanceColumn, newColName };

			// query for retrieving the second item of the list - the Actuals vs
			// Predicted
			String queryDataPoints = frameName;
			this.rJavaTranslator.executeEmptyR(queryDataPoints + "$ROW_ID <- seq.int(nrow(" + queryDataPoints + "))");
			List<Object[]> bulkRowDataPoints = this.rJavaTranslator.getBulkDataRow(queryDataPoints, dataTableHeaders);

			// create and return a task for the Actuals vs Predicted scatterplot
			ITask taskData = ConstantTaskCreationHelper.getScatterPlotData(panelId, "ROW_ID", instanceColumn,
					newColName, bulkRowDataPoints);
			this.insight.getTaskStore().addTask(taskData);

			// now return this frame change to include the predicted value
			// also return task object - for the Scatterplot of Actuals vs
			// Predicted
			// also throw success message
			NounMetadata noun = new NounMetadata(dataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
			noun.addAdditionalReturn(
					new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
			noun.addAdditionalReturn(new NounMetadata("Numerical Similarity ran successfully!",
					PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
			return noun;
		} else {
			NounMetadata noun = new NounMetadata("Numerical Similarity did not run successfully. No model found.",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			return noun;
		}

	}

	/*
	 * Run the algorithm Adds the new column to the frame
	 */
	private boolean runAlgorithm(String frameName, String instanceColumn, String newColName) {
		// the name of the result for merging later
		String resultFrame = "resultFrame" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// source the r script that will run the numerical correlation routine
		String NumSimScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\NumericalModel.R";
		NumSimScriptFilePath = NumSimScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + NumSimScriptFilePath + "\");\n");

		// get the trimmed frame name and then only run models on unique values
		// also only return non-null values
		rsb.append(resultFrame + " <- " + frameName + "$" + instanceColumn + ";\n");
		rsb.append(resultFrame + " <- na.omit(" + resultFrame + ");\n");
		rsb.append(resultFrame + " <- unique(" + resultFrame + ");\n");

		// run the script
		// and store prediction in a dataframe
		rsb.append(resultFrame + " <- compose_model( as.data.frame(" + resultFrame + ") );\n");
		rsb.append(resultFrame + " <- " + resultFrame + "$Comparison " + " ;\n");

		// Only merge frames if returned greater than 0 rows
		// if error, df will have 0 rows
		rsb.append("if(length(" + resultFrame + ") > 0) {");

		// merge the dataframes together into the results frame
		rsb.append(resultFrame + " <- merge( x=" + frameName + " , y=" + resultFrame + ", by.x = \"" + instanceColumn
				+ "\", by.y = \"Output\" , all=TRUE );");
		// remove unneeded columns and rename prediction column
		rsb.append(resultFrame + "$One <- NULL;");
		rsb.append(resultFrame + "$Two <- NULL;");
		rsb.append("setnames(" + resultFrame + ",old = c(\"NN_Output\"), new=c(\"" + newColName + "\"));");
		// reset the current frame
		rsb.append(frameName + " <- " + resultFrame + ";");

		rsb.append("}\n");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

		// add return to make sure that frame was changed
		int len = this.rJavaTranslator.getInt("nrow(" + resultFrame + ")");

		// garbage collection
		this.rJavaTranslator.executeEmptyR("rm(" + resultFrame + "); gc();");

		// return whether or not it was a success
		if (len == 0) {
			return false;
		} else {
			return true;
		}

	}

}