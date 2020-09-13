package prerna.algorithm.learning.r;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.PKQLRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RRoutine {

	/**
	 * This builder constructs an RRoutine object with or without optional
	 * parameters.
	 * 
	 * @author tbanach
	 *
	 */
	public static class Builder {

		/**
		 * Enumeration of each R routine type, used to set the script directory.
		 * Each type has its own script folder. For example, ANALYTICS scripts
		 * share the AnalyticsRoutineScripts folder and USER scripts share the
		 * UserScripts folder.
		 * 
		 * @author
		 *
		 */
		public enum RRoutineType {
			ANALYTICS, USER
		}

		// Required parameters
		private ITableDataFrame dataFrame;
		private PKQLRunner pkql;
		private String scriptName;
		private String rSyncFrameName;

		// Optional parameters
		private String selectedColumns;
		private String rReturnFrameName;
		private String[] arguments;
		private RRoutineType routineType;

		// Derived parameters
		private String scriptsDirectory;
		private String utilityScriptPath;

		/**
		 * Sets the default parameters for an RRoutine.
		 * 
		 * @param dataFrame
		 *            The ITableDataFrame that is synchronized to R as
		 *            rSyncFrameName
		 * @param pkql
		 *            The PKQLRunner used to execute Java reactor methods
		 * @param scriptName
		 *            The name of the script (ending in .R) that will be applied
		 *            to the data frame. RRoutine will look for this script in
		 *            (Semoss base folder)/R/AnalyticsRoutineScripts/
		 * @param rSyncFrameName
		 *            The name of the R frame synchronized to R
		 */
		public Builder(ITableDataFrame dataFrame, PKQLRunner pkql, String scriptName, String rSyncFrameName) {
			this.dataFrame = dataFrame;
			this.pkql = pkql;
			this.scriptName = scriptName;
			this.rSyncFrameName = rSyncFrameName;
		}

		/**
		 * Constructs a new RRoutine instance given the Builder's configuration.
		 * 
		 * @return RRoutine
		 */
		public RRoutine build() {
			String scriptFolder;
			if (routineType == null) {
				routineType = RRoutineType.USER;
			}
			switch (routineType) {
			case ANALYTICS:
				scriptFolder = Constants.R_ANALYTICS_SCRIPTS_FOLDER;
				break;
			case USER:
				scriptFolder = Constants.R_USER_SCRIPTS_FOLDER;
				break;
			default:
				scriptFolder = Constants.R_USER_SCRIPTS_FOLDER;
				break;
			}
			scriptsDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\"
					+ Constants.R_BASE_FOLDER + "\\" + scriptFolder;
			utilityScriptPath = scriptsDirectory + "\\" + Constants.R_UTILITY_SCRIPT;

			// If there is no return frame name, then use the sync frame name
			if (rReturnFrameName == null || rReturnFrameName.length() == 0) {
				rReturnFrameName = rSyncFrameName;
			}
			RRoutine rRoutine = new RRoutine(dataFrame, pkql, scriptName, rSyncFrameName, rReturnFrameName,
					scriptsDirectory, utilityScriptPath);
			rRoutine.selectedColumns = selectedColumns;
			rRoutine.rReturnFrameName = rReturnFrameName;
			rRoutine.arguments = arguments;
			return rRoutine;
		}

		/**
		 * Sets columns to select from the data frame when synchronizing to R
		 * (if the user does not wish to synchronize the entire frame). Column
		 * names are semicolon-delimited from the input string.
		 * 
		 * @param selectedColumns
		 *            The semicolon-delimited string of column names
		 * @return this Builder
		 */
		public Builder selectedColumns(String selectedColumns) {
			this.selectedColumns = selectedColumns;
			return this;
		}

		/**
		 * Sets the name of the R frame returned from R. If this optional
		 * parameter is not given, then the frame with the same frame name that
		 * was synchronized to R will be returned.
		 * 
		 * @param rReturnFrameName
		 * @return this Builder
		 */
		public Builder rReturnFrameName(String rReturnFrameName) {
			this.rReturnFrameName = rReturnFrameName;
			return this;
		}

		/**
		 * Sets arguments that are synchronized to R as args (if the user has
		 * arguments to synchronize). Arguments are semicolon-delimited from the
		 * input string. They are synchronized to R as a list, and can be
		 * accessed in the script via args[[i]], where i is the index of an
		 * argument. Note that string arguments must be surrounded by quotation
		 * marks. For example, the arguments string
		 * "'COUNT_1';0.01;'both';0.05;100;FALSE" synchronizes to R as: args <-
		 * list('COUNT_1', 0.01, 'both', 0.05, 100, FALSE). In this example,
		 * args[[1]] returns the string "COUNT_1", args[[2]] returns the number
		 * 0.01, and args[[6]] returns the boolean FALSE when executed in the R
		 * script.
		 * 
		 * @param arguments
		 *            The arguments string that will be synchronized to R as
		 *            args
		 * @return this Builder
		 */
		public Builder arguments(String arguments) {
			this.arguments = arguments.split(";");
			return this;
		}

		/**
		 * Sets the RRoutine type, used to set the script directory. Each type
		 * has its own script folder. For example, ANALYTICS scripts share the
		 * AnalyticsRoutineScripts folder and USER scripts share the UserScripts
		 * folder.
		 * 
		 * @param routineType
		 *            The RRoutineType that is used to set the script directory
		 * @return this Builder
		 */
		public Builder routineType(RRoutineType routineType) {
			this.routineType = routineType;
			return this;
		}
	}

	// Required
	private ITableDataFrame dataFrame;
	private PKQLRunner pkql;
	private String scriptName;
	private String rSyncFrameName;
	private String scriptsDirectory;
	private String utilityScriptPath;

	// Optional
	private String selectedColumns;
	private String rReturnFrameName;
	private String[] arguments;

	// Constants
	private static final String R_ARGS_VAR = "args";
	private static final Logger LOGGER = LogManager.getLogger(RRoutine.class.getName());

	private RRoutine(ITableDataFrame dataFrame, PKQLRunner pkql, String scriptName, String rSyncFrameName,
			String rReturnFrameName, String scriptsDirectory, String utilityScriptPath) {
		this.dataFrame = dataFrame;
		this.pkql = pkql;
		this.scriptName = scriptName;
		this.rSyncFrameName = rSyncFrameName;
		this.rReturnFrameName = rReturnFrameName;
		this.scriptsDirectory = scriptsDirectory;
		this.utilityScriptPath = utilityScriptPath;
	}

	/**
	 * Runs this routine by 1) synchronizing this RRoutine's frame to R, 2)
	 * sourcing utility functions from Utility.R for use, 3) synchronizing this
	 * RRoutine's arguments to R as a list, 4) running this RRoutine's .R
	 * script, 5) synchronizing a frame from R.
	 * 
	 * @return ITableDataFrame - The frame that is synchronized from R
	 * @throws RRoutineException
	 */
	public ITableDataFrame runRoutine() throws RRoutineException {

		// Source utility functions
		String path = utilityScriptPath.replace("\\", "/");
		LOGGER.info("Sourcing R utility functions from " + utilityScriptPath);
		r("source(\"" + path + "\");");

		// Synchronize the arguments list as args if there are any arguments
		if (arguments != null && arguments.length > 0) {
			String argumentsScript = "list(" + Arrays.stream(arguments).reduce((a, b) -> a + ", " + b).get() + ")";
			LOGGER.info("Syncronizing the arguments " + argumentsScript + " to R");
			r(R_ARGS_VAR + " <- " + argumentsScript);
		}

		// TODO implement selectors, for now just synchronize the entire frame
		if (selectedColumns != null && selectedColumns.length() != 0 && !selectedColumns.equals("*")) {
			// Then safe to use selectors
		}

		// Synchronize to R as rSyncFrameName, run the script, and synchronize
		// back
		try {

			// Stream the lines, trim white spaces, remove semicolons at the end
			// of lines (since they are re-added in reduce), check for
			// comments and remove, then reduce to a single semicolon delimited
			// string
			String script = Files.readAllLines(Paths.get(scriptsDirectory + "\\" + scriptName)).stream()
					.map(String::trim).map(l -> l.replaceAll(";$", ""))
					.filter(l -> (!l.startsWith("#") && !l.isEmpty())).reduce((a, b) -> a + ";" + b).get() + ";";
			LOGGER.info("Running the script " + script);
			script = script.replaceAll("\"", "\\\\\\\"");

			// synchronizeFromR uses the "GRID_NAME" variable to decide which
			// frame to push back
			j("synchronizeToR(\"" + rSyncFrameName + "\");runR(\"" + script + "\");storeVariable(\"GRID_NAME\", \""
					+ rReturnFrameName + "\");synchronizeFromR();");
		} catch (IOException e) {
			throw new RRoutineException("Failed to read " + scriptsDirectory + "\\" + scriptName, e);
		}

		// Return the output string
		LOGGER.info("Successfully ran R routine");
		return (ITableDataFrame) pkql.getDataFrame();
	}

	// Helper methods for runRoutine
	private void r(String script) throws RRoutineException {

		// \\ \\ \\ \" -> \\ \" -> \"
		script = script.replaceAll("\"", "\\\\\\\"");
		j("runR(\"" + script + "\");");
	}

	// Suppress warnings when grabbing the pqkl result
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void j(String java) throws RRoutineException {

		// Run the java code in a pkql
		pkql.runPKQL("meta.j: <code>" + java + "<code>;", dataFrame);

		// Retrieve the result
		List<Map> results = pkql.getResults();
		Map<String, Object> lastResult = results.get(results.size() - 1);

		// Parse the result
		String command = lastResult.get("command").toString();
		String status = lastResult.get("status").toString();

		// If there was an error running the pkql, throw a new exception
		if (status.equals("ERROR")) {
			throw new RRoutineException("Failed to run the command " + command);
		}
	}

}
