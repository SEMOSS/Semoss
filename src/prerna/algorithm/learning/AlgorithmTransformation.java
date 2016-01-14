package prerna.algorithm.learning;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.similarity.DatasetSimilarity;
import prerna.algorithm.learning.unsupervised.clustering.ClusteringRoutine;
import prerna.algorithm.learning.unsupervised.clustering.MultiClusteringRoutine;
import prerna.algorithm.learning.unsupervised.outliers.FastOutlierDetection;
import prerna.algorithm.learning.unsupervised.outliers.LOF;
import prerna.algorithm.learning.unsupervised.som.SOMRoutine;
import prerna.ui.components.playsheets.datamakers.AbstractTransformation;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

public class AlgorithmTransformation extends AbstractTransformation {

	private static final Logger LOGGER = LogManager.getLogger(AlgorithmTransformation.class.getName());
	private static final String TRANSFORMATION_NAME = "algorithmTransformation";			// the name to distinguish the the post transformation as an algorithm transformation in insight makeup
	private static final String METHOD_NAME = "performAnalyticTransformation";				// the name of the method in all data makers to perform an algorithm transformation
	public static final String UNDO_METHOD_NAME = "removeColumn";							// the name of the method in all data makers to perform an undo algorithm transformation
	public static final String ALGORITHM_TYPE = "algorithmType";							// key in the properties to determine the type of algorithm being run

	// the list of default algorithm transformations
	public static final String CLUSTERING = "clustering";
	public static final String MULTI_CLUSTERING = "multi_clustering";
	public static final String LOCAL_OUTLIER_FACTOR = "lof";
	public static final String FAST_OUTLIERS = "fast_outlier";
	public static final String SELF_ORGANIZING_MAP = "som";
	public static final String SIMILARITY = "similarity";

	private List<String> addedColumns = new ArrayList<String>();
	private DataMakerComponent dmc;
	private ITableDataFrame dm;

	@Override
	//TODO: need to figure out how the routines themselves will be obtained to override defaults in rdf map and engine prop
	public void runMethod() {
		Method method = null;
		try {
			method = dm.getClass().getMethod(METHOD_NAME, IAnalyticTransformationRoutine.class);
			LOGGER.info("Successfully got method : " + METHOD_NAME);
			String type = (String) this.props.get(ALGORITHM_TYPE);
			if(type == null) {
				throw new IllegalArgumentException("Algorithm type not specified for Algorithm Transformation");
			}

			IAnalyticTransformationRoutine routine = null;
			switch(type) {
			case CLUSTERING : routine = new ClusteringRoutine(); 
				break;
			case MULTI_CLUSTERING : routine = new MultiClusteringRoutine(); 
				break;
			case LOCAL_OUTLIER_FACTOR : routine = new LOF(); 
				break;
			case FAST_OUTLIERS : routine = new FastOutlierDetection(); 
				break;
			case SELF_ORGANIZING_MAP : routine = new SOMRoutine(); 
				break;
			case SIMILARITY : routine = new DatasetSimilarity(); 
				break;
			}
			if(routine == null) {
				throw new IllegalArgumentException("Algorithm routine " + type + " cannot be found.");
			}
			
			routine.setSelectedOptions(props);
			method.invoke(dm, routine);
			LOGGER.info("Successfully invoked method : " + METHOD_NAME);
			this.addedColumns.addAll(routine.getChangedColumns());
			
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			String message = "";
			if(e.getMessage()!= null && e.getMessage().isEmpty()) {
				message = e.getMessage();
			} else {
				message = e.getTargetException().getMessage();
			}
			throw new IllegalArgumentException(message);
		}
		return;

	}

	@Override
	public void setDataMakers(IDataMaker... dms) {
		this.dm = (ITableDataFrame) dms[0];
	}

	@Override
	public void setDataMakerComponent(DataMakerComponent dmc) {
		this.dmc = dmc;
	}

	@Override
	public void setTransformationType(Boolean preTransformation) {
		if(preTransformation){
			LOGGER.error("Cannot run performAction as pretransformation");
		}
	}

	@Override
	public void setProperties(Map<String, Object> props) {
		this.props = props;
	}

	@Override
	public Map<String, Object> getProperties() {
		props.put(TYPE, TRANSFORMATION_NAME);
		return this.props;
	}

	@Override
	public void undoTransformation() {
		Method method = null;
		try {
			method = dm.getClass().getMethod(UNDO_METHOD_NAME, String.class);
			LOGGER.info("Successfully got method : " + UNDO_METHOD_NAME);
			
			// iterate from root to top for efficiency in removing connections
			for(int i = addedColumns.size()-1; i >= 0; i--) {
				method.invoke(dm, addedColumns.get(i));
				LOGGER.info("Successfully invoked method : " + UNDO_METHOD_NAME);
			}
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
	}
	
	public List<String> getAddedColumns() {
		return this.addedColumns;
	}

	@Override
	public AlgorithmTransformation copy() {
		AlgorithmTransformation copy = new AlgorithmTransformation();

		copy.setDataMakerComponent(this.dmc);
		copy.setDataMakers(this.dm);
		copy.setId(this.id);
		copy.addedColumns = this.addedColumns;

		if(this.props != null) {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String propCopy = gson.toJson(this.props);
			Map<String, Object> newProps = gson.fromJson(propCopy, new TypeToken<Map<String, Object>>() {}.getType());
			copy.setProperties(newProps);
		}
		return copy;
	}
}
