package prerna.algorithm.learning.r;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.r.RRoutine.Builder.RRoutineType;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.h2.H2Frame;
import prerna.ds.util.FileIterator;
import prerna.ds.util.FileIterator.FILE_DATA_TYPE;
import prerna.poi.main.HeadersException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class runs an R script on a data frame to produce a new data frame and
 * R-object result.
 * 
 * @author
 *
 */
public class RRoutine {

	/**
	 * This builder constructs an RRoutine object with or without optional
	 * parameters.
	 * 
	 * @author
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

		private ITableDataFrame dataFrame;
		private String scriptName;
		private String rSyncFrameName;
		private String[] selectedColumns;
		private String rReturnFrameName;
		private String[] arguments;
		private RRoutineType routineType;

		/**
		 * Sets the default parameters for an RRoutine.
		 * 
		 * @param dataFrame
		 *            The ITableDataFrame that is synchronized to R as
		 *            rSyncFrameName
		 * @param scriptName
		 *            The name of the script (ending in .R) that will be applied
		 *            to the data frame. RRoutine will look for this script in
		 *            (Semoss base folder)/R/AnalyticsRoutineScripts/
		 * @param rSyncFrameName
		 *            The name of the R frame synchronized to R
		 */
		public Builder(ITableDataFrame dataFrame, String scriptName, String rSyncFrameName) {
			this.dataFrame = dataFrame;
			this.scriptName = scriptName;
			this.rSyncFrameName = rSyncFrameName;
		}

		/**
		 * Constructs a new RRoutine instance given the Builder's configuration.
		 * 
		 * @return RRoutine
		 */
		public RRoutine build() {
			RRoutine rRoutine = new RRoutine(dataFrame, scriptName, rSyncFrameName);
			rRoutine.rReturnFrameName = rReturnFrameName;
			rRoutine.selectedColumns = selectedColumns;
			rRoutine.arguments = arguments;
			rRoutine.routineType = routineType;
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
			this.selectedColumns = selectedColumns.split(";");
			return this;
		}

		/**
		 * Sets the name of the R frame returned from R. If this optional
		 * parameter is not given, then the frame with the same frame name that
		 * was synchronized to R will be returned.
		 * 
		 * @param rReturnFrameName
		 * @return
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

	// User given
	// Required
	private ITableDataFrame dataFrame;
	private String scriptName;
	private String rSyncFrameName;

	// Optional
	private String[] selectedColumns;
	private String rReturnFrameName;
	private String[] arguments;
	private RRoutineType routineType;

	// Defined in constructor
	private String baseDirectory;
	private String scriptsDirectory;
	private String utilityScriptPath;
	private String driversDirectory;
	private String h2ClassPath;
	private String tempDirectory;
	private String[] headers;
	private IMetaData.DATA_TYPES[] types;
	private String tableName;

	// Rserve connections
	private RConnection rMasterConnection;
	private RConnection rserveConnection;

	// Constants
	private static final String R_BASE_FOLDER = "R";
	private static final String ANALYTICS_SCRIPTS_FOLDER = "AnalyticsRoutineScripts";
	private static final String USER_SCRIPTS_FOLDER = "UserScripts";
	private static final String UTILITY_SCRIPT = "Utility.R";
	private static final String DRIVERS_FOLDER = "RDFGraphLib";
	private static final String TEMP_FOLDER = "Temp";
	private static final String MASTER_HOST = "127.0.0.1";
	private static final int MASTER_PORT = 6311; // Default Rserve port
	private static final String DRIVER_VAR = "driver";
	private static final String DRIVER_CONNECTION_VAR = "connection";
	private static final String H2_DRIVER = "org.h2.Driver";
	private static final String H2_JAR = "h2-1.4.185.jar";
	private static final String H2_USERNAME = "sa";
	private static final String R_ARGS_VAR = "args";
	private static final String R_GET_RESULT_FUN = "GetResult();";

	// Results
	// Also keep track of whether the routine has been run,
	// to avoid computing the results more than once
	private boolean runRoutineCompleted = false;
	private H2Frame dataFrameResult;
	private REXP rObjectResult;

	private static final Logger LOGGER = LogManager.getLogger(RRoutine.class.getName());

	/**
	 * Constructs a new immutable RRoutine object. This constructor only accepts
	 * required parameters. To add optional parameters, use RRoutine.Builder.
	 * 
	 * @param dataFrame
	 *            The ITableDataFrame that is synchronized to R as
	 *            rSyncFrameName
	 * @param scriptName
	 *            The name of the script (ending in .R) that will be applied to
	 *            the data frame. RRoutine will look for this script in (Semoss
	 *            base folder)/R/AnalyticsRoutineScripts/
	 * @param rSyncFrameName
	 *            The name of the R frame synchronized to R
	 */
	public RRoutine(ITableDataFrame dataFrame, String scriptName, String rSyncFrameName) {
		this.dataFrame = dataFrame;
		this.scriptName = scriptName;
		this.rSyncFrameName = rSyncFrameName;

		// Determine variables that are independent of optional parameters
		// Since optional parameters are set by RRoutine's builder after
		// constructing the instance, any variables that depend on optional
		// parameters must be determined after constructing the instance
		// This is done in determineScriptsDirectory method
		baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		driversDirectory = baseDirectory + "\\" + DRIVERS_FOLDER;
		h2ClassPath = driversDirectory + "\\" + H2_JAR;
		tempDirectory = baseDirectory + "\\" + R_BASE_FOLDER + "\\" + TEMP_FOLDER;
		headers = dataFrame.getColumnHeaders();
		types = new IMetaData.DATA_TYPES[headers.length];
		for (int i = 0; i < headers.length; i++) {
			types[i] = dataFrame.getDataType(headers[i]);
		}
		tableName = dataFrame.getTableName();
		LOGGER.setLevel(Level.INFO);
	}

	/**
	 * Runs this RRoutine's R script on the data frame (if not already
	 * completed), then returns the transformed frame. The data frame is
	 * synchronized to R using this RRoutine's rSyncFrameName. The R frame given
	 * by rReturnFrameName after running the R script is stored in this RRoutine
	 * instance and returned as a new frame.
	 * 
	 * RRoutine will only execute its R script once, to avoid calculating the
	 * same results more than once (RRoutine is an immutable object; therefore,
	 * the same RRoutine instance should always produces the same result). If
	 * returnDataFrame() or returnResult() are called after the script has been
	 * executed (by a previous call to either of these methods), they will
	 * return the result stored in this RRoutine instance, rather than
	 * re-compute it.
	 * 
	 * @return ITableDataFrame - The new data frame that is the result of
	 *         running the R routine
	 * @throws RRoutineException
	 */
	public ITableDataFrame returnDataFrame() throws RRoutineException {
		if (runRoutineCompleted) {
			return dataFrameResult;
		} else {
			runRoutine();
			return dataFrameResult;
		}
	}

	/**
	 * Runs this RRoutine's R script on the data frame (if not already
	 * completed), then returns the transformed frame. The data frame is
	 * synchronized to R using this RRoutine's rSyncFrameName. If this
	 * RRoutine's R script defines a GetResult() function that returns the
	 * result of some analysis on the data frame, then the result is stored in
	 * this RRoutine instance as an R object (REXP) and returned. A default
	 * GetResult() function that returns null is also sourced before executing
	 * this RRoutine's script, in case this RRoutine's script does not define
	 * (essentially override) this function.
	 * 
	 * RRoutine will only execute its R script once, to avoid calculating the
	 * same results more than once (RRoutine is an immutable object; therefore,
	 * the same RRoutine instance should always produces the same result). If
	 * returnDataFrame() or returnResult() are called after the script has been
	 * executed (by a previous call to either of these methods), they will
	 * return the result stored in this RRoutine instance, rather than
	 * re-compute it.
	 * 
	 * @return REXP - The R object that is the result of running the R routine
	 * @throws RRoutineException
	 */
	public REXP returnResult() throws RRoutineException {
		if (runRoutineCompleted) {
			return rObjectResult;
		} else {
			runRoutine();
			return rObjectResult;
		}
	}

	// Runs the routine and stores whether the routine has been completed
	private void runRoutine() throws RRoutineException {

		// TODO implement other frame types
		if (!(dataFrame instanceof H2Frame)) {
			throw new RRoutineException("Data must be an H2Frame");
		}

		// Cannot be done in constructor, because the scriptsDirectory depends
		// on the optional parameter routineType
		determineScriptsDirectory();

		// All the below helper methods throw an RRoutineException and are
		// uncaught, except for connectToR() and initializeDriver(), which throw
		// RserveExceptions
		// They are caught at this level so that a finally block can be used to
		// close connections in the event of an error
		try {
			connectToR();
			sourceUtilityFunctions();
			synchronizeArgumentsToR();
			try {

				// For now, I am enforcing that the frame must be H2
				// Therefore it is safe to cast the frame as an H2Frame
				String url1 = ((H2Frame) dataFrame).getBuilder().connectFrame();
				url1 = url1.replace("\\", "/");

				// Initialize the H2 driver to facilitate synchronization
				// between Java and R
				initializeDriver(H2_DRIVER, h2ClassPath, url1, H2_USERNAME);

				// Synchronize to R and run the script
				synchronizeFrameToR();
				runScript();

				// Retrieve the results of the routine
				retrieveFrameFromR();
				retrieveResultFromR();

				// Mark that the routine has already been run
				runRoutineCompleted = true;
				LOGGER.info("Successfully ran R routine");
			} catch (RserveException e) {
				throw new RRoutineException("Failed to initialize driver", e);
			} finally {

				// Clean up connections
				try {
					disconnectDriver();
				} catch (RserveException e) {
					LOGGER.warn("Failed to disconnect driver");
					e.printStackTrace();
				}

				// Disconnect the frame but don't close the connection
				// The thread will disconnect, but the frame will still be
				// available to connect to in the event of an error
				// If an error is thrown, RRoutine will not return a new frame,
				// and the old one should still be available to connect to
				((H2Frame) dataFrame).getBuilder().disconnectFrame();
			}
		} catch (RserveException e) {
			throw new RRoutineException("Failed to connect to Rserve", e);
		} finally {

			// Clean up connections
			try {
				closeRConnection();
			} catch (RserveException e) {
				LOGGER.warn("Failed to close connection to R");
				e.printStackTrace();
			}
		}
	}

	// Helper methods for runRoutine

	// Set the scripts directory based on the routine type
	// If routine type is omitted, then use the user scripts folder by
	// default
	private void determineScriptsDirectory() {
		String scriptFolder;
		if (routineType == null) {
			routineType = RRoutineType.USER;
		}
		switch (routineType) {
		case ANALYTICS:
			scriptFolder = ANALYTICS_SCRIPTS_FOLDER;
			break;
		case USER:
			scriptFolder = USER_SCRIPTS_FOLDER;
			break;
		default:
			scriptFolder = USER_SCRIPTS_FOLDER;
			break;
		}
		scriptsDirectory = baseDirectory + "\\" + R_BASE_FOLDER + "\\" + scriptFolder;
		utilityScriptPath = scriptsDirectory + "\\" + UTILITY_SCRIPT;
	}

	// Sources an R script with utility functions,
	// so that they are available when executing the script
	// Includes default GetResult() function that returns null
	private void sourceUtilityFunctions() throws RRoutineException {
		try {
			String path = utilityScriptPath.replace("\\", "/");
			r("source(\"" + path + "\");");
			LOGGER.info("Sourced R utility functions from " + utilityScriptPath);
		} catch (RserveException e) {
			throw new RRoutineException("Failed to source R utility functions from " + utilityScriptPath, e);
		}
	}

	// Synchronizes the arguments string to R as args (args can be a list,
	// number, text, etc. depending on the string)
	private void synchronizeArgumentsToR() throws RRoutineException {
		if (arguments != null && arguments.length > 0) {
			String argumentsScript = "list(";
			for (int i = 0; i < arguments.length - 1; i++) {
				argumentsScript += arguments[i] + ", ";
			}
			argumentsScript += arguments[arguments.length - 1] + ")";
			try {
				r(R_ARGS_VAR + " <- " + argumentsScript + ";");
				LOGGER.info("Syncronized the arguments " + argumentsScript + " to R");
			} catch (RserveException e) {
				throw new RRoutineException("Failed to synchronize the arguments " + argumentsScript + " to R", e);
			}
		}
	}

	// Synchronizes the frame to R as rSyncFrameName
	private void synchronizeFrameToR() throws RRoutineException {

		// Select all by default (*)
		// If there are selected columns, then use them as selectors
		String selectors = "*";
		if (selectedColumns != null && selectedColumns.length > 0) {
			selectors = "";
			for (int i = 0; i < selectedColumns.length - 1; i++) {
				selectors += selectedColumns[i] + ", ";
			}
			selectors += selectedColumns[selectedColumns.length - 1];
		}
		try {

			// R code that synchronizes the frame to R
			r(rSyncFrameName + " <- as.data.table(unclass(dbGetQuery(" + DRIVER_CONNECTION_VAR + ", 'SELECT "
					+ selectors + " FROM " + tableName + "')));");
			r("setDT(" + rSyncFrameName + ");");
			LOGGER.info("Synchronized frame to R as " + rSyncFrameName);
		} catch (RserveException e) {
			throw new RRoutineException("Failed to synchronize dataframe to R", e);
		}
	}

	// Runs the .R script by streaming in the file
	private void runScript() throws RRoutineException {
		try {

			// Stream the lines, trim white spaces, remove semicolons at the end
			// of lines (since they are re-added in reduce), check for
			// comments and remove, then reduce to a single semicolon delimited
			// string
			String script = Files.readAllLines(Paths.get(scriptsDirectory + "\\" + scriptName)).stream()
					.map(String::trim).map(l -> l.replaceAll(";$", ""))
					.filter(l -> (!l.startsWith("#") && !l.isEmpty())).reduce((a, b) -> a + ";" + b).get();
			try {
				r(script);
				LOGGER.info("Successfully ran the script " + script);
			} catch (RserveException e) {
				throw new RRoutineException("Failed to run the script: " + script, e);
			}
		} catch (IOException e) {
			throw new RRoutineException("Failed to read " + scriptsDirectory + "\\" + scriptName, e);
		}
	}

	// Retrieves the frame given by rReturnFrameName if this parameter is
	// defined, otherwise returns the frame that was synchronized to R
	// It is much simpler to create an entirely new frame,
	// so that is what I am doing here (Occam's Razor)
	private void retrieveFrameFromR() throws RRoutineException {

		// If the rReturnFrameName is undefined,
		// then retrieve the frame that was synchronized to R
		if (rReturnFrameName == null || rReturnFrameName.length() == 0) {
			rReturnFrameName = rSyncFrameName;
		}

		try {

			// Get the headers and types from the R frame
			String[] headers = determineRHeaders(rReturnFrameName);
			String[] types = determineRStringTypes(rReturnFrameName);

			// Clean the headers
			// Semoss is more particular than R with frame headers
			List<String> cleanHeaders = new Vector<String>();
			HeadersException headerException = HeadersException.getInstance();
			for (int i = 0; i < headers.length; i++) {
				String cleanHeader = headerException.recursivelyFixHeaders(headers[i], cleanHeaders);
				cleanHeaders.add(cleanHeader);
			}
			headers = cleanHeaders.toArray(new String[] {});

			// Create a data type map and a query structure
			QueryStruct qs = new QueryStruct();
			Map<String, IMetaData.DATA_TYPES> dataTypeMap = new Hashtable<String, IMetaData.DATA_TYPES>();
			Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
			for (int i = 0; i < headers.length; i++) {
				dataTypeMapStr.put(headers[i], types[i]);
				dataTypeMap.put(headers[i], Utility.convertStringToDataType(types[i]));
				qs.addSelector(headers[i], null);
			}

			// Create a new H2Frame with the proper structure
			Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
			dataFrameResult = new H2Frame();
			dataFrameResult.mergeEdgeHash(edgeHash, dataTypeMapStr);

			// Use a temp file file (faster than in memory synchronization)
			String tempFileLocation = tempDirectory;
			tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
			tempFileLocation = tempFileLocation.replace("\\", "/");
			r("fwrite(" + rReturnFrameName + ", file='" + tempFileLocation + "');");

			// Iterate through the temp file and insert values
			FileIterator dataIterator = FileIterator.createInstance(FILE_DATA_TYPE.META_DATA_ENUM, tempFileLocation,
					',', qs, dataTypeMap);
			dataFrameResult.addRowsViaIterator(dataIterator, dataTypeMap);
			dataIterator.deleteFile();

			// Update the user id to match the new schema
			dataFrameResult.setUserId(dataFrameResult.getSchema());
			LOGGER.info("Retrived the data frame " + rReturnFrameName + " from R");
		} catch (RserveException | REXPMismatchException e) {
			throw new RRoutineException("Failed to retrieve data frame from R", e);
		}
	}

	// Retrieves the R-object result from R using the GetResult() function
	private void retrieveResultFromR() throws RRoutineException {
		try {
			rObjectResult = r(R_GET_RESULT_FUN);
			LOGGER.info("Retrieved result from R");
		} catch (RserveException e) {
			throw new RRoutineException("Failed to retrieve result from R", e);
		}
	}

	// Methods to connect to, and disconnect from, R processes

	// Makes a new Rserve connection on the master
	private void connectToR() throws RserveException {

		// Connect to an external R process (master)
		rMasterConnection = new RConnection(MASTER_HOST, MASTER_PORT);

		// Find an open port for Rserve
		int rservePort = Integer.parseInt(Utility.findOpenPort());

		// Start Rserve on the master (creates a separate workspace)
		rMasterConnection.eval("library(Rserve); Rserve(port = " + rservePort + ")");

		// Give the master one second to start Rserve
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		// Connect to Rserve
		rserveConnection = new RConnection(MASTER_HOST, rservePort);
		r("library(splitstackshape);");
		r("library(data.table);");
		r("library(reshape2);");
		r("library(RJDBC);");
		LOGGER.info("Connected to Rserve on port " + rservePort);
	}

	// Runs r
	private REXP r(String script) throws RserveException {
		LOGGER.debug("Evaluating the script: " + script);
		return rserveConnection.eval(script);
	}

	// Closes the Rserve connection when finished running the R routine (or if
	// there is some other unexpected error)
	private void closeRConnection() throws RserveException {

		// Gracefully shutdown and close the Rserve connection
		if (rserveConnection != null && rserveConnection.isConnected()) {
			rserveConnection.shutdown();
			rserveConnection.close();
		}

		// Close the master connection
		// The master is an external process,
		// and cannot be shutdown client side
		if (rMasterConnection != null && rMasterConnection.isConnected()) {
			rMasterConnection.close();
		}
		LOGGER.info("Closed connections to R");
	}

	// Initializes the driver in R that will facilitate synchronization
	private void initializeDriver(String driver, String classPath, String url, String username) throws RserveException {

		// Replace backslashes with forward slashes in directories
		classPath = classPath.replace("\\", "/");
		url = url.replace("\\", "/");

		// See https://cran.r-project.org/web/packages/RJDBC/RJDBC.pdf
		r(DRIVER_VAR + " <- JDBC('" + driver + "', '" + classPath + "', identifier.quote='`');");
		r(DRIVER_CONNECTION_VAR + " <- dbConnect(" + DRIVER_VAR + ", '" + url + "', '" + username + "', '');");
		LOGGER.info("Initialized driver for " + url);
	}

	// Disconnects the driver
	private void disconnectDriver() throws RserveException {
		r("dbDisconnect(" + DRIVER_CONNECTION_VAR + ");");
		LOGGER.info("Disconnected driver");
	}

	// Determines the headers of an R frame
	private String[] determineRHeaders(String frameName) throws RserveException, REXPMismatchException {
		return r("matrix(colnames(" + frameName + "));").asStrings();
	}

	// Determines the types of an R frame (as strings - 'numeric', 'character',
	// or 'Date')
	private String[] determineRStringTypes(String frameName) throws RserveException, REXPMismatchException {
		return r("matrix(sapply(" + frameName + ", class));").asStrings();
	}

}
