package prerna.reactor.runtime;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerAlgorithmUtility;
import prerna.ds.TinkerFrame;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Console;
import prerna.util.DIHelper;

public abstract class AbstractJavaReactorBaseClass extends AbstractReactor {

	protected Logger logger = null;

	public Console System = new Console();
	public SecurityManager curManager = null;
	public SecurityManager reactorManager = null;
	
	public List<NounMetadata> nounMetaOutput = new Vector<NounMetadata>();
	public ITableDataFrame dataframe;
	
	/**
	 * Method to execute
	 * @return
	 */
	public void runCompiledCode() {
		try {
			execute();
		} catch(Exception ex) {
			throw ex;
		}
	}
	
	public List<NounMetadata> getNounMetaOutput() {
		return this.nounMetaOutput;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////// Frame Methods ///////////////////////
	
	/**
	 * Filter node to set of values
	 * @param columnHeader
	 * @param instances
	 */
	protected void filterNode(String columnHeader, String[] instances) {
		List<Object> values = new Vector<Object>();
		for(int instanceIndex = 0;instanceIndex < instances.length; instanceIndex++) {
			values.add(instances[instanceIndex]);
		}
		NounMetadata colMeta = new NounMetadata(columnHeader, PixelDataType.COLUMN);
		NounMetadata valuesMeta = new NounMetadata(values, PixelDataType.CONST_STRING);
		SimpleQueryFilter filter = new SimpleQueryFilter(colMeta, "==", valuesMeta);
		this.dataframe.addFilter(filter);
		
		// set the output so the FE knows what occurred
		this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_FILTER));
	}
	
	/**
	 * Filter node to set of values
	 * @param columnHeader
	 * @param instances
	 */
	protected void filterNode(String columnHeader, List<Object> values) {
		NounMetadata colMeta = new NounMetadata(columnHeader, PixelDataType.COLUMN);
		NounMetadata valuesMeta = new NounMetadata(values, PixelDataType.CONST_STRING);
		SimpleQueryFilter filter = new SimpleQueryFilter(colMeta, "==", valuesMeta);
		((ITableDataFrame) this.dataframe).addFilter(filter);
		
		// set the output so the FE knows what occurred
		this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_FILTER));
	}
	
	/**
	 * Filter note to specific value
	 * @param columnHeader
	 * @param value
	 */
	protected void filterNode(String columnHeader, String value) {
		NounMetadata colMeta = new NounMetadata(columnHeader, PixelDataType.COLUMN);
		NounMetadata valuesMeta = new NounMetadata(value, PixelDataType.CONST_STRING);
		SimpleQueryFilter filter = new SimpleQueryFilter(colMeta, "==", valuesMeta);
		((ITableDataFrame) this.dataframe).addFilter(filter);
		
		// set the output so the FE knows what occurred
		this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_FILTER));
	}

//	/**
//	 * 
//	 * Runs anomaly detection on a numeric series.
//	 * 
//	 * @param timeColumn
//	 *            The column containing time stamps; can be date, string, or
//	 *            numeric representation
//	 * @param seriesColumn
//	 *            The column containing a numeric series with potential
//	 *            anomalies
//	 * @param aggregateFunction
//	 *            The function used to aggregate the series when there are
//	 *            duplicated time stamps
//	 * @param maxAnoms
//	 *            The maximum proportion of the series of counts that can be
//	 *            considered an anomaly, must be between 0 and 1
//	 * @param direction
//	 *            The direction in which anomalies can occur, includes POSITIVE,
//	 *            NEGATIVE, and BOTH
//	 * @param alpha
//	 *            The level of statistical significance, must be between 0 and
//	 *            1, but should generally be less than 0.1
//	 * @param period
//	 *            The number of time stamps per natural cycle; anomalies are
//	 *            sensitive to this input
//	 * @param keepExistingColumns
//	 *            Whether to keep the existing column structure and add to it,
//	 *            or return a simplified data frame
//	 */
//	protected void runAnomaly(String timeColumn, String seriesColumn, String aggregateFunction, double maxAnoms,
//			String direction, double alpha, int period, boolean keepExistingColumns) {
//		java.lang.System.setSecurityManager(curManager);
//
//		// Convert string direction to AnomDirection
//		// Default to both
//		AnomDirection anomDirection;
//		switch (direction) {
//		case "positive":
//			anomDirection = AnomDirection.POSITIVE;
//			break;
//		case "negative":
//			anomDirection = AnomDirection.NEGATIVE;
//			break;
//		default:
//			anomDirection = AnomDirection.BOTH;
//			break;
//		}
//
//		// Create a new anomaly detector
//		AnomalyDetector anomalyDetector = new AnomalyDetector(dataframe, pkql, timeColumn, seriesColumn,
//				aggregateFunction, maxAnoms, anomDirection, alpha, period, keepExistingColumns);
//
//		// Detect anomalies using the anomaly detector
//		try {
//			anomalyDetector.detectAnomalies();
//		} catch (RRoutineException e) {
//			e.printStackTrace();
//		}
//		java.lang.System.setSecurityManager(reactorManager);
//	}
	
//	/**
//	 * 
//	 * Runs anomaly detection on categorical data. This is done by counting the
//	 * number of events that occur per unit time for each group.
//	 * 
//	 * @param timeColumn
//	 *            The column containing time stamps; can be date, string, or
//	 *            numeric representation
//	 * @param eventColumn
//	 *            The column containing events to count, usually the primary key
//	 *            when counting records
//	 * @param groupColumn
//	 *            The column to group by; the count of events is reported for
//	 *            each level of this group
//	 * @param aggregateFunction
//	 *            The function used to aggregate events, count or count distinct
//	 * @param maxAnoms
//	 *            The maximum proportion of the series of counts that can be
//	 *            considered an anomaly, must be between 0 and 1
//	 * @param direction
//	 *            The direction in which anomalies can occur, includes POSITIVE,
//	 *            NEGATIVE, and BOTH
//	 * @param alpha
//	 *            The level of statistical significance, must be between 0 and
//	 *            1, but should generally be less than 0.1
//	 * @param period
//	 *            The number of time stamps per natural cycle; anomalies are
//	 *            sensitive to this input
//	 */
//	protected void runCategoricalAnomaly(String timeColumn, String eventColumn, String groupColumn,
//			String aggregateFunction, double maxAnoms, String direction, double alpha, int period) {
//		java.lang.System.setSecurityManager(curManager);
//
//		// Convert string direction to AnomDirection
//		// Default to both
//		AnomDirection anomDirection;
//		switch (direction) {
//		case "positive":
//			anomDirection = AnomDirection.POSITIVE;
//			break;
//		case "negative":
//			anomDirection = AnomDirection.NEGATIVE;
//			break;
//		default:
//			anomDirection = AnomDirection.BOTH;
//			break;
//		}
//
//		// Create a new anomaly detector
//		AnomalyDetector anomalyDetector = new AnomalyDetector(dataframe, pkql, timeColumn, eventColumn, groupColumn,
//				aggregateFunction, maxAnoms, anomDirection, alpha, period);
//
//		// Detect anomalies using the anomaly detector
//		try {
//			anomalyDetector.detectAnomalies();
//		} catch (RRoutineException e) {
//			e.printStackTrace();
//		}
//		java.lang.System.setSecurityManager(reactorManager);
//	}
		
//	/**
//	 * Runs a .R file script with the current data frame in Semoss synchronized
//	 * to R as rSyncFrameName. After running the routine, the R frame with name
//	 * rReturnFrameName is synchronized back to Semoss.
//	 * 
//	 * @param scriptName
//	 *            The name of the script in (Semoss base folder)\R\UserScripts
//	 *            to run
//	 * @param rSyncFrameName
//	 *            The name of the R data frame that is synchronized to R
//	 * @param selectedColumns
//	 *            A semicolon-delimited string of columns to select when
//	 *            synchronizing the frame to R ("*" for all)
//	 * @param rReturnFrameName
//	 *            The name of the R data frame that is synchronized from R
//	 * @param arguments
//	 *            A semicolon-delimited string of arguments that are
//	 *            synchronized to R as an args list. These arguments can then be
//	 *            accessed in R via args[[i]], where i is the index of the
//	 *            desired argument
//	 */
//	protected void runRRoutine(String scriptName, String rSyncFrameName, String selectedColumns,
//			String rReturnFrameName, String arguments) {
//		java.lang.System.setSecurityManager(curManager);
//		RRoutine rRoutine = new RRoutine.Builder(dataframe, pkql, scriptName, rSyncFrameName)
//				.selectedColumns(selectedColumns).rReturnFrameName(rReturnFrameName).arguments(arguments).build();
//		try {
//			rRoutine.runRoutine();
//		} catch (RRoutineException e) {
//			e.printStackTrace();
//		}
//		// Data ID is updated when the frame is synchronized from R
//
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//
//	protected void runRRoutine(String scriptName, String rSyncFrameName, String selectedColumns,
//			String rReturnFrameName) {
//		java.lang.System.setSecurityManager(curManager);
//		RRoutine rRoutine = new RRoutine.Builder(dataframe, pkql, scriptName, rSyncFrameName)
//				.selectedColumns(selectedColumns).rReturnFrameName(rReturnFrameName).build();
//		try {
//			rRoutine.runRoutine();
//		} catch (RRoutineException e) {
//			e.printStackTrace();
//		}
//		// Data ID is updated when the frame is synchronized from R
//
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//
//	protected void runRRoutine(String scriptName, String rSyncFrameName, String selectedColumns) {
//		java.lang.System.setSecurityManager(curManager);
//		RRoutine rRoutine = new RRoutine.Builder(dataframe, pkql, scriptName, rSyncFrameName)
//				.selectedColumns(selectedColumns).build();
//		try {
//			rRoutine.runRoutine();
//		} catch (RRoutineException e) {
//			e.printStackTrace();
//		}
//		// Data ID is updated when the frame is synchronized from R
//
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//
//	protected void runRRoutine(String scriptName, String rSyncFrameName) {
//		java.lang.System.setSecurityManager(curManager);
//		RRoutine rRoutine = new RRoutine.Builder(dataframe, pkql, scriptName, rSyncFrameName).build();
//		try {
//			rRoutine.runRoutine();
//		} catch (RRoutineException e) {
//			e.printStackTrace();
//		}
//		// Data ID is updated when the frame is synchronized from R
//
//		java.lang.System.setSecurityManager(reactorManager);
//	}

	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	//////////////////////// Tinker Methods ////////////////////////
	
	/**
	 * Remove all nodes of a specific type
	 * @param type
	 */
	protected void removeNode(String type) {
		java.lang.System.setSecurityManager(curManager);
		if(this.dataframe instanceof TinkerFrame)
		{
			((TinkerFrame)this.dataframe).removeColumn(type);
			String output = "Removed nodes for " + type;
			System.out.println(output);
			this.dataframe.updateDataId();
			// set the output so the FE knows what occurred
			this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Remove all nodes of a specific type and with a specific value
	 * @param type
	 * @param data
	 */
	protected void removeNode(String type, String data) {
		java.lang.System.setSecurityManager(curManager);
		if(this.dataframe instanceof TinkerFrame) {
			List<Object> removeList = new Vector<Object>();
			removeList.add(data);
			((TinkerFrame)this.dataframe).remove(type, removeList);
			String output = "Removed nodes for  " + data + " with values " + removeList;
			System.out.println(output);
			this.dataframe.updateDataId();
			// set the output so the FE knows what occurred
			this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Find degree of a node
	 * @param type
	 * @param data
	 */
	protected void degree(String type, String data) {
		java.lang.System.setSecurityManager(curManager);
		if(this.dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)this.dataframe).degree(type, data);
			String output = "Degrees for  " + data + ":" + degree;
			System.out.println(output);
			// set the output so the FE knows what occurred
			this.nounMetaOutput.add(new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION));
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Calculate the node eigenvalue
	 * @param type
	 * @param data
	 */
	protected void eigen(String type, String data)
	{
		java.lang.System.setSecurityManager(curManager);
		if(this.dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)this.dataframe).eigen(type, data);
			String output = "Eigen for  " + data + ":" +degree;
			System.out.println(output);
			// set the output so the FE knows what occurred
			this.nounMetaOutput.add(new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION));
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Determine if node is an orphan
	 * @param type
	 * @param data
	 */
	protected void isOrphan(String type, String data) {
		java.lang.System.setSecurityManager(curManager);
		if(this.dataframe instanceof TinkerFrame)
		{
			boolean orphan = ((TinkerFrame)this.dataframe).isOrphan(type, data);
			String output = data + "  Orphan? " + orphan;
			System.out.println(output);
			// set the output so the FE knows what occurred
			this.nounMetaOutput.add(new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION));
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
//	/**
//	 * Generate a new graph based of the edges to traverse
//	 * @param selectors
//	 * @param edges
//	 */
//	protected void generateNewGraph(String edgeHashStr, String traversalHashStr) {
//		java.lang.System.setSecurityManager(curManager);
//		if(this.dataframe instanceof TinkerFrame)
//		{
//			TinkerFrame newDataFrame = DataFrameHelper.generateNewGraph((TinkerFrame) this.dataframe, edgeHashStr, traversalHashStr);
//			if(newDataFrame == null) {
//				System.out.println("ERROR: Generated graph is empty! Please modify inputs to get a frame which has data");
//			} else {
//				// set the output so the FE knows what occurred
//				this.nounMetaOutput.add(new NounMetadata(newDataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
//				System.out.println("Generated new graph data frame");
//			}
//		}		
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//	
//	/**
//	 * Find shared vertices between group of instances with a certain number of traversals away
//	 * @param type
//	 * @param instances
//	 * @param numTraversals
//	 */
//	protected void findSharedVertices(String type, String[] instances, int numTraversals) {
//		java.lang.System.setSecurityManager(curManager);
//		if(this.dataframe instanceof TinkerFrame)
//		{	
//			TinkerFrame newDataFrame = DataFrameHelper.findSharedVertices((TinkerFrame) this.dataframe, type, instances, numTraversals);
//			// set the output so the FE knows what occurred
//			this.nounMetaOutput.add(new NounMetadata(newDataFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
//			System.out.println("Generated new graph data frame");
//			
//			newDataFrame.updateDataId();
//			System.out.println("Filtered to keep only vertices which are shared between defined instances");
//		}		
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//
//	/**
//	 * Shifts a tinker node into a node property
//	 * @param conceptName
//	 * @param propertyName
//	 * @param traversal
//	 */
//	protected void shiftToNodeProperty(String conceptName, String propertyName, Map<String, Set<String>> traversal) {
//		java.lang.System.setSecurityManager(curManager);
//		if(this.dataframe instanceof TinkerFrame)
//		{
//			DataFrameHelper.shiftToNodeProperty((TinkerFrame) this.dataframe, conceptName, propertyName, traversal);
//			this.dataframe.updateDataId();
//			System.out.println("Modified graph data frame");
//			
//			// set the output so the FE knows what occurred
//			this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
//		}		
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//	
//	protected void shiftToNodeProperty(String conceptName, String propertyName, String traversal) {
//		java.lang.System.setSecurityManager(curManager);
//		if(this.dataframe instanceof TinkerFrame)
//		{
//			DataFrameHelper.shiftToNodeProperty((TinkerFrame) this.dataframe, conceptName, propertyName, traversal);
//			this.dataframe.updateDataId();
//			System.out.println("Modified graph data frame");
//			
//			// set the output so the FE knows what occurred
//			this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
//		}		
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//	
//	/**
//	 * Shifts a tinker node into an edge property
//	 * @param conceptName
//	 * @param propertyName
//	 * @param traversal
//	 */
//	protected void shiftToEdgeProperty(String[] relationship, String propertyName, Map<String, Set<String>> traversal) {
//		java.lang.System.setSecurityManager(curManager);
//		if(this.dataframe instanceof TinkerFrame)
//		{
//			DataFrameHelper.shiftToEdgeProperty((TinkerFrame) this.dataframe, relationship, propertyName, traversal);
//			this.dataframe.updateDataId();
//			System.out.println("Modified graph data frame");
//
//			// set the output so the FE knows what occurred
//			this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
//		}		
//		java.lang.System.setSecurityManager(reactorManager);
//	}
//	
//	protected void shiftToEdgeProperty(String relationship, String propertyName, String traversal) {
//		java.lang.System.setSecurityManager(curManager);
//		if(this.dataframe instanceof TinkerFrame)
//		{
//			DataFrameHelper.shiftToEdgeProperty((TinkerFrame) this.dataframe, relationship, propertyName, traversal);
//			this.dataframe.updateDataId();
//			System.out.println("Modified graph data frame");
//			
//			// set the output so the FE knows what occurred
//			this.nounMetaOutput.add(new NounMetadata(this.dataframe, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE));
//		}		
//		java.lang.System.setSecurityManager(reactorManager);
//	}

	protected void runLoopIdentifer(int cycleSize) {
		java.lang.System.setSecurityManager(curManager);
		if(this.dataframe instanceof TinkerFrame)
		{
			String loops = TinkerAlgorithmUtility.runLoopIdentifer((TinkerFrame) this.dataframe, cycleSize);
			System.out.println(loops);
			
			// set the output so the FE knows what occurred
			this.nounMetaOutput.add(new NounMetadata(loops, PixelDataType.CONST_STRING, PixelOperationType.OPERATION));
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runDisconnectedNodesIdentifier(String type, String instance) {
		java.lang.System.setSecurityManager(curManager);
		if(this.dataframe instanceof TinkerFrame)
		{
			String disconnectedNodes = TinkerAlgorithmUtility.runDisconnectedNodesIdentifier((TinkerFrame) this.dataframe, type, instance);
			System.out.println(disconnectedNodes);

			// set the output so the FE knows what occurred
			this.nounMetaOutput.add(new NounMetadata(disconnectedNodes, PixelDataType.CONST_STRING, PixelOperationType.OPERATION));
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	//////////////////// Setter / Getters //////////////////////
	
	protected void setCurSecurityManager(SecurityManager curManager) {
		this.curManager = curManager;
	}
	
	protected void setReactorManager(SecurityManager reactorManager) {
		this.reactorManager = reactorManager;
	}
	
	protected void setConsole() {
		this.System = new Console();
	}
	
	protected Object retrieveVariable(String varName) {
		NounMetadata noun = this.insight.getVarStore().get(varName);
		if(noun == null) {
			return null;
		}
		return noun.getValue();
	}
	
	protected void storeVariable(String varName, NounMetadata noun) {
		this.insight.getVarStore().put(varName, noun);
	}
	
	protected void setDataFrame(ITableDataFrame frame) {
		this.dataframe = frame;
	}
	
	protected void removeVariable(String varName) {
		this.insight.getVarStore().remove(varName);
	}
	
	/**
	 * Get the base folder
	 * @return
	 */
	protected String getBaseFolder() {
		String baseFolder = null;
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		} catch (Exception ignored) {
			logger.info("No BaseFolder detected... most likely running as test...");
		}
		if(baseFolder == null)
			baseFolder = "C:/users/pkapaleeswaran/workspacej3/SemossWeb";
		
		return baseFolder;
	}
	
	protected void setLogger(Logger logger) {
		this.logger = logger;
	}
	
}
