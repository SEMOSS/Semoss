package prerna.sablecc;

import java.sql.Connection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.r.RRoutine;
import prerna.algorithm.learning.r.RRoutineException;
import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector;
import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector.AnomDirection;
import prerna.ds.DataFrameHelper;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.sqlserver.SqlServerFrame;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Console;
import prerna.util.DIHelper;

public abstract class AbstractJavaReactor extends AbstractReactor {

	protected static final Logger LOGGER = LogManager.getLogger(AbstractJavaReactor.class.getName());

	public PKQLRunner pkql = new PKQLRunner();

	public boolean frameChanged = false;
	public ITableDataFrame dataframe = null;

	public boolean hasReturnData = false;
	public Object returnData;

	public SecurityManager curManager = null;
	public SecurityManager reactorManager = null;
	
	public String userId = null;
	
	public String wd = null;
	public String fileName = null;
	
	public Console System = new Console();
	
	/**
	 * Default constructor defines an empty frame
	 */
	public AbstractJavaReactor() {
		this.dataframe = new H2Frame();
	}
	
	/**
	 * Default constructor defines an empty frame
	 */
	public AbstractJavaReactor(ITableDataFrame dataframe) {
		this.dataframe = dataframe;
		this.userId = dataframe.getUserId();
	}
	
	/**
	 * Method to be run
	 */
	public void doMethod() {
		
	}

	@Override
	public Iterator process() {
		try {
			doMethod();
			put("RESPONSE", System.out.output); 
			put("STATUS" , prerna.sablecc.PKQLRunner.STATUS.SUCCESS);
		} catch(Exception ex) {
			if( ex.getMessage() != null && !ex.getMessage().isEmpty() ) {
				put("RESPONSE", "ERROR : " + ex.getMessage());
			} else {
				put("RESPONSE", "Failed");
			}
			put("STATUS", prerna.sablecc.PKQLRunner.STATUS.ERROR);
			put("ERROR", ex);
			return null;
		}

		return null;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////// Frame Methods ///////////////////////
	
	/**
	 * Refresh the data frame
	 */
	protected void refresh() {
		frameChanged = true;
		dataframe.updateDataId();
	}
	
	/**
	 * Execute a pkql command from the java console
	 * @param pkqlString
	 */
	protected void runPKQL(String pkqlString) {
		System.out.println("Running pkql.. " + pkqlString);
		pkql.runPKQL(pkqlString, dataframe);
		myStore.put("RESPONSE", System.out.output);
	}
	
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
		dataframe.filter(columnHeader, values);
	}
	
	/**
	 * Filter node to set of values
	 * @param columnHeader
	 * @param instances
	 */
	protected void filterNode(String columnHeader, List<Object> instances) {
		dataframe.filter(columnHeader, instances);		
	}
	
	/**
	 * Filter note to specific value
	 * @param columnHeader
	 * @param value
	 */
	protected void filterNode(String columnHeader, String value) {
		List<Object> values = new Vector<Object>();
		values.add(value);
		dataframe.filter(columnHeader, values);
	}

	/**
	 * 
	 * Runs anomaly detection on a numeric series.
	 * 
	 * @param timeColumn
	 *            The column containing time stamps; can be date, string, or
	 *            numeric representation
	 * @param seriesColumn
	 *            The column containing a numeric series with potential
	 *            anomalies
	 * @param aggregateFunction
	 *            The function used to aggregate the series when there are
	 *            duplicated time stamps
	 * @param maxAnoms
	 *            The maximum proportion of the series of counts that can be
	 *            considered an anomaly, must be between 0 and 1
	 * @param direction
	 *            The direction in which anomalies can occur, includes POSITIVE,
	 *            NEGATIVE, and BOTH
	 * @param alpha
	 *            The level of statistical significance, must be between 0 and
	 *            1, but should generally be less than 0.1
	 * @param period
	 *            The number of time stamps per natural cycle; anomalies are
	 *            sensitive to this input
	 * @param keepExistingColumns
	 *            Whether to keep the existing column structure and add to it,
	 *            or return a simplified data frame
	 */
	protected void runAnomaly(String timeColumn, String seriesColumn, String aggregateFunction, double maxAnoms,
			String direction, double alpha, int period, boolean keepExistingColumns) {
		java.lang.System.setSecurityManager(curManager);

		// Convert string direction to AnomDirection
		// Default to both
		AnomDirection anomDirection;
		switch (direction) {
		case "positive":
			anomDirection = AnomDirection.POSITIVE;
			break;
		case "negative":
			anomDirection = AnomDirection.NEGATIVE;
			break;
		default:
			anomDirection = AnomDirection.BOTH;
			break;
		}

		// Create a new anomaly detector
		AnomalyDetector anomalyDetector = new AnomalyDetector(dataframe, timeColumn, seriesColumn, aggregateFunction,
				maxAnoms, anomDirection, alpha, period, keepExistingColumns);

		// Detect anomalies using the anomaly detector
		try {
			int oldId = dataframe.getDataId();
			dataframe = anomalyDetector.detectAnomalies();

			// Update the data id if it is not already updated
			if (oldId != 1) {
				dataframe.updateDataId();
			}
			frameChanged = true;
			myStore.put("G", dataframe);
		} catch (RRoutineException e) {
			e.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
	}

	/**
	 * 
	 * Runs anomaly detection on categorical data. This is done by counting the
	 * number of events that occur per unit time for each group.
	 * 
	 * @param timeColumn
	 *            The column containing time stamps; can be date, string, or
	 *            numeric representation
	 * @param eventColumn
	 *            The column containing events to count, usually the primary key
	 *            when counting records
	 * @param groupColumn
	 *            The column to group by; the count of events is reported for
	 *            each level of this group
	 * @param aggregateFunction
	 *            The function used to aggregate events, count or count distinct
	 * @param maxAnoms
	 *            The maximum proportion of the series of counts that can be
	 *            considered an anomaly, must be between 0 and 1
	 * @param direction
	 *            The direction in which anomalies can occur, includes POSITIVE,
	 *            NEGATIVE, and BOTH
	 * @param alpha
	 *            The level of statistical significance, must be between 0 and
	 *            1, but should generally be less than 0.1
	 * @param period
	 *            The number of time stamps per natural cycle; anomalies are
	 *            sensitive to this input
	 */
	protected void runCategoricalAnomaly(String timeColumn, String eventColumn, String groupColumn,
			String aggregateFunction, double maxAnoms, String direction, double alpha, int period) {
		java.lang.System.setSecurityManager(curManager);

		// Convert string direction to AnomDirection
		// Default to both
		AnomDirection anomDirection;
		switch (direction) {
		case "positive":
			anomDirection = AnomDirection.POSITIVE;
			break;
		case "negative":
			anomDirection = AnomDirection.NEGATIVE;
			break;
		default:
			anomDirection = AnomDirection.BOTH;
			break;
		}

		// Create a new anomaly detector
		AnomalyDetector anomalyDetector = new AnomalyDetector(dataframe, timeColumn, eventColumn, groupColumn,
				aggregateFunction, maxAnoms, anomDirection, alpha, period);

		// Detect anomalies using the anomaly detector
		try {
			int oldId = dataframe.getDataId();
			dataframe = anomalyDetector.detectAnomalies();

			// Update the data id if it is not already updated
			if (oldId != 1) {
				dataframe.updateDataId();
			}
			frameChanged = true;
			myStore.put("G", dataframe);
		} catch (RRoutineException e) {
			e.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
	}
		
	protected void runClustering(int instanceIndex, int numClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.ClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.ClusteringReactor.NUM_CLUSTERS.toUpperCase(), numClusters);
		
		prerna.algorithm.impl.ClusteringReactor alg = new prerna.algorithm.impl.ClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runClustering(String columnName, int numClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		if(instanceIndex == -1) {
			int selectorsLength = selectors.length;
			String[] newSelectors = new String[selectorsLength+1];
			newSelectors[0] = columnName;
			instanceIndex = 0;
			
			for(int i = 0; i < selectorsLength; i++) {
				newSelectors[i+1] = selectors[i];
			}
			
			// reassign the new values
			selectors = newSelectors;
			instanceIndex = 0;
		}
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.ClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.ClusteringReactor.NUM_CLUSTERS.toUpperCase(), numClusters);
		
		prerna.algorithm.impl.ClusteringReactor alg = new prerna.algorithm.impl.ClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runMultiClustering(int instanceIndex, int minNumClusters, int maxNumClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.MultiClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MIN_NUM_CLUSTERS.toUpperCase(), minNumClusters);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MAX_NUM_CLUSTERS.toUpperCase(), maxNumClusters);

		prerna.algorithm.impl.MultiClusteringReactor alg = new prerna.algorithm.impl.MultiClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runMultiClustering(String columnName, int minNumClusters, int maxNumClusters, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		if(instanceIndex == -1) {
			int selectorsLength = selectors.length;
			String[] newSelectors = new String[selectorsLength+1];
			newSelectors[0] = columnName;
			instanceIndex = 0;
			
			for(int i = 0; i < selectorsLength; i++) {
				newSelectors[i+1] = selectors[i];
			}
			
			// reassign the new values
			selectors = newSelectors;
			instanceIndex = 0;
		}
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.MultiClusteringReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MIN_NUM_CLUSTERS.toUpperCase(), minNumClusters);
		params.put(prerna.algorithm.impl.MultiClusteringReactor.MAX_NUM_CLUSTERS.toUpperCase(), maxNumClusters);

		prerna.algorithm.impl.MultiClusteringReactor alg = new prerna.algorithm.impl.MultiClusteringReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runLOF(int instanceIndex, int k, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.LOFReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.LOFReactor.K_NEIGHBORS.toUpperCase(), k);
		
		prerna.algorithm.impl.LOFReactor alg = new prerna.algorithm.impl.LOFReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runLOF(String columnName, int k, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		if(instanceIndex == -1) {
			int selectorsLength = selectors.length;
			String[] newSelectors = new String[selectorsLength+1];
			newSelectors[0] = columnName;
			instanceIndex = 0;
			
			for(int i = 0; i < selectorsLength; i++) {
				newSelectors[i+1] = selectors[i];
			}
			
			// reassign the new values
			selectors = newSelectors;
			instanceIndex = 0;
		}
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.LOFReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.LOFReactor.K_NEIGHBORS.toUpperCase(), k);
		
		prerna.algorithm.impl.LOFReactor alg = new prerna.algorithm.impl.LOFReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runOutlier(int instanceIndex, int numSubsetSize, int numRums, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.OutlierReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.OutlierReactor.NUM_SAMPLE_SIZE.toUpperCase(), numSubsetSize);
		params.put(prerna.algorithm.impl.OutlierReactor.NUMBER_OF_RUNS.toUpperCase(), numRums);
		
		prerna.algorithm.impl.OutlierReactor alg = new prerna.algorithm.impl.OutlierReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}	
	
	protected void runOutlier(String columnName, int numSubsetSize, int numRums, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		if(instanceIndex == -1) {
			int selectorsLength = selectors.length;
			String[] newSelectors = new String[selectorsLength+1];
			newSelectors[0] = columnName;
			instanceIndex = 0;
			
			for(int i = 0; i < selectorsLength; i++) {
				newSelectors[i+1] = selectors[i];
			}
			
			// reassign the new values
			selectors = newSelectors;
			instanceIndex = 0;
		}
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.OutlierReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		params.put(prerna.algorithm.impl.OutlierReactor.NUM_SAMPLE_SIZE.toUpperCase(), numSubsetSize);
		params.put(prerna.algorithm.impl.OutlierReactor.NUMBER_OF_RUNS.toUpperCase(), numRums);
		
		prerna.algorithm.impl.OutlierReactor alg = new prerna.algorithm.impl.OutlierReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Runs a .R file script with the current data frame in Semoss synchronized
	 * to R as rSyncFrameName. After running the routine, the R frame with name
	 * rReturnFrameName is synchronized back to Semoss.
	 * 
	 * @param scriptName
	 *            The name of the script in (Semoss base folder)\R\UserScripts
	 *            to run
	 * @param rSyncFrameName
	 *            The name of the R data frame that is synchronized to R
	 * @param selectedColumns
	 *            A semicolon-delimited string of columns to select when
	 *            synchronizing the frame to R ("*" for all)
	 * @param rReturnFrameName
	 *            The name of the R data frame that is synchronized from R
	 * @param arguments
	 *            A semicolon-delimited string of arguments that are
	 *            synchronized to R as an args list. These arguments can then be
	 *            accessed in R via args[[i]], where i is the index of the
	 *            desired argument
	 */
	protected void runRRoutine(String scriptName, String rSyncFrameName, String selectedColumns,
			String rReturnFrameName, String arguments) {
		java.lang.System.setSecurityManager(curManager);
		RRoutine rRoutine = new RRoutine.Builder(dataframe, scriptName, rSyncFrameName).selectedColumns(selectedColumns)
				.rReturnFrameName(rReturnFrameName).arguments(arguments).build();
		try {
			int oldId = dataframe.getDataId();
			dataframe = rRoutine.returnDataFrame();

			// Update the data id if it is not already updated
			if (oldId != 1) {
				dataframe.updateDataId();
			}
			frameChanged = true;
			myStore.put("G", dataframe);
		} catch (RRoutineException e) {
			e.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
	}

	protected void runRRoutine(String scriptName, String rSyncFrameName, String selectedColumns,
			String rReturnFrameName) {
		java.lang.System.setSecurityManager(curManager);
		RRoutine rRoutine = new RRoutine.Builder(dataframe, scriptName, rSyncFrameName).selectedColumns(selectedColumns)
				.rReturnFrameName(rReturnFrameName).build();
		try {
			int oldId = dataframe.getDataId();
			dataframe = rRoutine.returnDataFrame();

			// Update the data id if it is not already updated
			if (oldId != 1) {
				dataframe.updateDataId();
			}
			frameChanged = true;
			myStore.put("G", dataframe);
		} catch (RRoutineException e) {
			e.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
	}

	protected void runRRoutine(String scriptName, String rSyncFrameName, String selectedColumns) {
		java.lang.System.setSecurityManager(curManager);
		RRoutine rRoutine = new RRoutine.Builder(dataframe, scriptName, rSyncFrameName).selectedColumns(selectedColumns)
				.build();
		try {
			int oldId = dataframe.getDataId();
			dataframe = rRoutine.returnDataFrame();

			// Update the data id if it is not already updated
			if (oldId != 1) {
				dataframe.updateDataId();
			}
			frameChanged = true;
			myStore.put("G", dataframe);
		} catch (RRoutineException e) {
			e.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
	}

	protected void runRRoutine(String scriptName, String rSyncFrameName) {
		java.lang.System.setSecurityManager(curManager);
		RRoutine rRoutine = new RRoutine(dataframe, scriptName, rSyncFrameName);
		try {
			int oldId = dataframe.getDataId();
			dataframe = rRoutine.returnDataFrame();

			// Update the data id if it is not already updated
			if (oldId != 1) {
				dataframe.updateDataId();
			}
			frameChanged = true;
			myStore.put("G", dataframe);
		} catch (RRoutineException e) {
			e.printStackTrace();
		}
		java.lang.System.setSecurityManager(reactorManager);
	}

	protected void runSimilarity(int instanceIndex, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.SimilarityReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		
		prerna.algorithm.impl.SimilarityReactor alg = new prerna.algorithm.impl.SimilarityReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void runSimilarity(String columnName, String[] selectors) {
		java.lang.System.setSecurityManager(curManager);
		
		int instanceIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(selectors, columnName);
		if(instanceIndex == -1) {
			int selectorsLength = selectors.length;
			String[] newSelectors = new String[selectorsLength+1];
			newSelectors[0] = columnName;
			instanceIndex = 0;
			
			for(int i = 0; i < selectorsLength; i++) {
				newSelectors[i+1] = selectors[i];
			}
			
			// reassign the new values
			selectors = newSelectors;
			instanceIndex = 0;
		}
		
		java.util.Map<String, Object> params = new java.util.Hashtable<String, Object>();
		params.put(prerna.algorithm.impl.SimilarityReactor.INSTANCE_INDEX.toUpperCase(), instanceIndex);
		
		prerna.algorithm.impl.SimilarityReactor alg = new prerna.algorithm.impl.SimilarityReactor();
		alg.put("G", this.dataframe);
		alg.put(PKQLEnum.MAP_OBJ, params);
		alg.put(PKQLEnum.COL_DEF, java.util.Arrays.asList(selectors));
		alg.process();
		
		this.dataframe.updateDataId();
		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	//////////////////////// Tinker Methods ////////////////////////
	
	/**
	 * Remove all nodes of a specific type
	 * @param type
	 */
	protected void removeNode(String type) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			((TinkerFrame)dataframe).removeColumn(type);
			String output = "Removed nodes for " + type;
			System.out.println(output);
			dataframe.updateDataId();
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
		if(dataframe instanceof TinkerFrame) {
			List<Object> removeList = new Vector<Object>();
			removeList.add(data);
			((TinkerFrame)dataframe).remove(type, removeList);
			String output = "Removed nodes for  " + data + " with values " + removeList;
			System.out.println(output);
			dataframe.updateDataId();
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
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).degree(type, data);
			String output = "Degrees for  " + data + ":" + degree;
			System.out.println(output);
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
		if(dataframe instanceof TinkerFrame)
		{
			Object degree = ((TinkerFrame)dataframe).eigen(type, data);
			String output = "Eigen for  " + data + ":" +degree;
			System.out.println(output);
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
		if(dataframe instanceof TinkerFrame)
		{
			boolean orphan = ((TinkerFrame)dataframe).isOrphan(type, data);
			String output = data + "  Orphan? " + orphan;
			System.out.println(output);
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Generate a new graph based of the edges to traverse
	 * @param selectors
	 * @param edges
	 */
	protected void generateNewGraph(String[] selectors, Map<String, String> edges) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			TinkerFrame newDataFrame = DataFrameHelper.generateNewGraph((TinkerFrame) dataframe, selectors, edges);
			myStore.put("G", newDataFrame);
			System.out.println("Generated new graph data frame");
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Find shared vertices between group of instances with a degree of freedom
	 * @param type
	 * @param instances
	 * @param degree
	 */
	protected void findSharedVertices(String type, String[] instances, int degree) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{	
			TinkerFrame newDataFrame = DataFrameHelper.findSharedVertices((TinkerFrame) dataframe, type, instances, degree);
			myStore.put("G", newDataFrame);
			newDataFrame.updateDataId();
			System.out.println("Filtered to keep only vertices which are shared between defined instances");
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}

	/**
	 * Shifts a tinker node into a node property
	 * @param conceptName
	 * @param propertyName
	 * @param traversal
	 */
	protected void shiftToNodeProperty(String conceptName, String propertyName, Map<String, Set<String>> traversal) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			DataFrameHelper.shiftToNodeProperty((TinkerFrame) dataframe, conceptName, propertyName, traversal);
			dataframe.updateDataId();
			System.out.println("Modified graph data frame");
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void shiftToNodeProperty(String conceptName, String propertyName, String traversal) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			DataFrameHelper.shiftToNodeProperty((TinkerFrame) dataframe, conceptName, propertyName, traversal);
			dataframe.updateDataId();
			System.out.println("Modified graph data frame");
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	/**
	 * Shifts a tinker node into an edge property
	 * @param conceptName
	 * @param propertyName
	 * @param traversal
	 */
	protected void shiftToEdgeProperty(String[] relationship, String propertyName, Map<String, Set<String>> traversal) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			DataFrameHelper.shiftToEdgeProperty((TinkerFrame) dataframe, relationship, propertyName, traversal);
			dataframe.updateDataId();
			System.out.println("Modified graph data frame");
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	protected void shiftToEdgeProperty(String relationship, String propertyName, String traversal) {
		java.lang.System.setSecurityManager(curManager);
		if(dataframe instanceof TinkerFrame)
		{
			DataFrameHelper.shiftToEdgeProperty((TinkerFrame) dataframe, relationship, propertyName, traversal);
			dataframe.updateDataId();
			System.out.println("Modified graph data frame");
		}		
		java.lang.System.setSecurityManager(reactorManager);
	}
	
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	//////////////////////// H2Frame Methods ///////////////////////

	protected Connection getConnection() {
		if(dataframe instanceof H2Frame) {
			return ((H2Frame)dataframe).getBuilder().getConnection();
		} else {
			return null;
		}
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	/////////////////// Sql Server Methods /////////////////////
	
	protected void setSqlServerFrame(String tableName) {
		java.lang.System.setSecurityManager(curManager);
		SqlServerFrame frame = new SqlServerFrame();
		frame.connectToExistingTable(tableName);
		
		// ugh... why are there so many references to this thing!!!!!
		this.dataframe = frame;
		this.dataframe.updateDataId();
		this.frameChanged = true;
		myStore.put("G", frame);
		
		System.out.println("Successfully connected to table name = '" + tableName + "'");
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
	
	/**
	 * Set a variable
	 * @param varName
	 * @param value
	 */
	protected void storeVariable(String varName, Object value) {
		// set a variable
		// use this variable if it is needed on the next call
		pkql.setVariableValue(varName, value);
	}
	
	/**
	 * Get a variable
	 * @param varName
	 * @return
	 */
	protected Object retrieveVariable(String varName) {
		// get the variable back that was set
		return pkql.getVariableValue(varName);
	}
	
	/**
	 * Remove a variable
	 * @param varName
	 */
	protected void removeVariable(String varName) {
		pkql.removeVariable(varName);
	}

	/**
	 * Set the pkql runner
	 * @param pkql
	 */
	protected void setPKQLRunner(PKQLRunner pkql) {
		this.pkql = pkql;
	}
	
	/**
	 * Set the data frame
	 * @param dataFrame
	 */
	protected void setDataFrame(ITableDataFrame dataFrame) {
		this.dataframe = dataFrame;
		this.userId = dataframe.getUserId();
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
			LOGGER.info("No BaseFolder detected... most likely running as test...");
		}
		if(baseFolder == null)
			baseFolder = "C:/users/pkapaleeswaran/workspacej3/SemossWeb";
		
		return baseFolder;
	}
	
	////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////
	////////////////////// Reactor Methods /////////////////////
	
	@Override
	public Object getValue(String key) {
		return myStore.get(key);
	}

	@Override
	public void put(String key, Object value) {
		myStore.put(key, value);
	}
	
	@Override
	public String[] getParams() {
		return null;
	}

	@Override
	public void set(String key, Object value) {
	}

	@Override
	public void addReplacer(String pattern, Object value) {
	}

	@Override
	public String[] getValues2Sync(String childName) {
		return null;
	}

	@Override
	public void removeReplacer(String pattern) {
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		return null;
	}
}
